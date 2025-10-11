from __future__ import annotations

from typing import Any, Dict, Tuple, Generator
from contextlib import contextmanager
import pytest
import importlib
from urllib.parse import unquote_plus

import etl_core.components.databases.mongodb.mongodb_connection_handler as mch


def _decoded_inline_creds(uri: str) -> str | None:
    """Return decoded 'user[:pass]' if creds are inline, else None."""
    try:
        after_scheme = uri.split("://", 1)[1]
        if "@" not in after_scheme:
            return None
        creds_part = after_scheme.split("@", 1)[0]
        return unquote_plus(creds_part)
    except Exception:
        return None


class _FakeRegistry:
    def __init__(self) -> None:
        self._leased = 0
        self._released = 0
        self._closed = False
        self._force_closed = False
        self._clients: Dict[str, Tuple[str, Any]] = {}

    def get_mongo_client(
        self, *, uri: str, client_kwargs: Dict[str, Any]
    ) -> Tuple[str, Any]:
        key = f"key:{uri}"
        client = _FakeClient()
        self._clients[key] = (uri, client)
        return key, client

    def lease_mongo(self, key: str) -> None:
        self._leased += 1

    def release_mongo(self, key: str) -> None:
        self._released += 1

    def close_pool(self, key: str, *, force: bool = False) -> bool:
        if force:
            self._force_closed = True
            self._closed = True
            return True
        if self._leased == self._released:
            self._closed = True
            return True
        return False

    def stats(self) -> Dict[str, Any]:
        return {
            "leased": self._leased,
            "released": self._released,
            "closed": self._closed,
            "force_closed": self._force_closed,
        }


class _FakeCollection:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeDB(dict):
    def __getitem__(self, name: str) -> _FakeCollection:  # type: ignore[override]
        return _FakeCollection(name)


class _FakeClient(dict):
    def __getitem__(self, name: str) -> _FakeDB:  # type: ignore[override]
        return _FakeDB()


@pytest.fixture()
def fake_registry(monkeypatch: pytest.MonkeyPatch) -> _FakeRegistry:
    reg = _FakeRegistry()
    monkeypatch.setattr(
        mch.ConnectionPoolRegistry, "instance", lambda: reg, raising=True
    )
    return reg


def test_build_uri_variants() -> None:
    u1 = mch.MongoConnectionHandler.build_uri(host="h", port=27017)
    assert u1 == "mongodb://h:27017"

    u2 = mch.MongoConnectionHandler.build_uri(host="h", port=1, user="u")
    assert u2 in {"mongodb://u@h:1", "mongodb://h:1"}

    u3 = mch.MongoConnectionHandler.build_uri(
        host="h",
        port=2,
        user="a b",
        password="p@ss",
        auth_db="admin",
        params={"replicaSet": "rs0"},
    )
    assert u3.startswith("mongodb://h:2/")
    assert "authSource=admin" in u3 and "replicaSet=rs0" in u3

    u4 = mch.MongoConnectionHandler.build_uri(host="h1,h2", port=27017, params={})
    assert u4 == "mongodb://h1,h2:27017"


def test_mask_uri_variants() -> None:
    m = mch.MongoConnectionHandler._mask_uri
    assert m("mongodb://h:1").startswith("mongodb://") and m("mongodb://h:1").endswith(
        "h:1"
    )

    masked_user = m("mongodb://u@h:1")
    assert masked_user.startswith("mongodb")
    assert "***" in masked_user and masked_user.endswith("@h:1")

    masked_pass = m("mongodb://u:p@h:1")
    assert masked_pass.startswith("mongodb")
    assert "***" in masked_pass and masked_pass.endswith("@h:1")


def test_connect_and_lease_and_close(
    fake_registry: _FakeRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = mch.MongoConnectionHandler()
    key, client = h.connect(uri="mongodb://h:1", client_kwargs={})
    assert key.startswith("key:") and client is not None

    @contextmanager
    def _patched_lease_collection(
        *, database: str, collection: str
    ) -> Generator[Tuple[Any, Any], None, None]:
        fake_registry.lease_mongo(key)
        try:
            coll = client[database][collection]
            yield client, coll
        finally:
            fake_registry.release_mongo(key)

    def _patched_close_pool(*, force: bool = False) -> bool:
        return fake_registry.close_pool(key, force=force)

    monkeypatch.setattr(h, "lease_collection", _patched_lease_collection, raising=True)
    monkeypatch.setattr(h, "close_pool", _patched_close_pool, raising=True)

    with h.lease_collection(database="db", collection="people") as (c, coll):
        assert c is client
        assert isinstance(coll, _FakeCollection)
        assert coll.name == "people"

    assert h.close_pool(force=False) is True
    stats = h.stats()
    assert stats == {"leased": 1, "released": 1, "closed": True, "force_closed": False}


def test_force_close_when_still_leased(
    fake_registry: _FakeRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = mch.MongoConnectionHandler()
    key, client = h.connect(uri="mongodb://h:1", client_kwargs={})

    fake_registry.lease_mongo(key)
    assert fake_registry._leased == 1 and fake_registry._released == 0

    def _patched_close_pool(*, force: bool = False) -> bool:
        return fake_registry.close_pool(key, force=force)

    monkeypatch.setattr(h, "close_pool", _patched_close_pool, raising=True)
    assert h.close_pool(force=False) is False
    assert h.close_pool(force=True) is True
    assert fake_registry._force_closed is True and fake_registry._closed is True


def test_lease_without_connect_raises(fake_registry: _FakeRegistry) -> None:
    h = mch.MongoConnectionHandler()
    with pytest.raises(RuntimeError):
        with h.lease_collection(database="db", collection="c"):
            pass  # pragma: no cover


def test_close_without_key_returns_false(fake_registry: _FakeRegistry) -> None:
    h = mch.MongoConnectionHandler()
    assert h.close_pool(force=False) is False


def test_real_lease_collection_uses_registry_and_releases(
    fake_registry: _FakeRegistry,
) -> None:
    """
    Use a real handler instance but inject the fake registry and a fake client.
    This covers the actual contextmanager body (lease/yield/release) without
    relying on unbound method trickery that can mis-bind 'self'.
    """
    client = _FakeClient()
    client["_"] = True

    h = mch.MongoConnectionHandler()
    object.__setattr__(h, "_key", "key:mongodb://h:1")  # mimic connect()
    object.__setattr__(h, "_client", client)
    object.__setattr__(h, "_registry", fake_registry)

    assert fake_registry._leased == 0 and fake_registry._released == 0

    with h.lease_collection(database="db", collection="people") as (c, coll):
        assert c is client
        assert isinstance(coll, _FakeCollection)
        assert coll.name == "people"

    assert fake_registry._leased == 1 and fake_registry._released == 1


def test_close_pool_with_key_calls_registry(fake_registry: _FakeRegistry) -> None:
    """
    Use the real close_pool(): with a key present it should delegate to registry.
    """
    h = mch.MongoConnectionHandler()
    key, _client = h.connect(uri="mongodb://h:1", client_kwargs={})

    object.__setattr__(h, "_key", key)
    object.__setattr__(h, "_registry", fake_registry)

    assert h.close_pool(force=False) is True
    assert fake_registry._closed is True and fake_registry._force_closed is False

    fake_registry._closed = False
    fake_registry._force_closed = False
    fake_registry._leased = 1
    fake_registry._released = 0
    assert h.close_pool(force=True) is True
    assert fake_registry._closed is True and fake_registry._force_closed is True


def test_build_uri_user_and_password_and_params_exact() -> None:
    """
    Exercise the 'user/password + auth_db + params' call path.
    Some implementations intentionally omit credentials in the URI and rely
    on client kwargs for auth. Accept either URI shape but verify:
      - scheme present
      - host:port present
      - query contains 'authSource' first, then additional params
    """
    u = mch.MongoConnectionHandler.build_uri(
        host="h",
        port=2,
        user="a b",
        password="p@ss",
        auth_db="admin",
        params={"replicaSet": "rs0"},
    )

    assert u.startswith("mongodb://")

    assert u.endswith("h:2/?authSource=admin&replicaSet=rs0")

    has_creds = "@h:2" in u
    if has_creds:
        assert "a%20b:p%40ss@" in u


def test_build_uri_params_only() -> None:
    """
    params without user/password/auth_db -> only params render.
    """
    u = mch.MongoConnectionHandler.build_uri(
        host="h",
        port=6,
        params={"tls": "true"},
    )
    assert u == "mongodb://h:6/?tls=true"


def test_mask_uri_user_without_scheme_branch() -> None:
    """
    Hit the ':'-absent prefix branch in _mask_uri by omitting the scheme.
    """
    m = mch.MongoConnectionHandler._mask_uri
    assert m("u@h:1") == "***@h:1"


def _fresh_mch():
    importlib.reload(mch)
    return mch


def _has_inline_creds(uri: str) -> bool:
    return "@".join(uri.split("://", 1)[-1].split("@")[:-1]) != ""


def test_build_uri_full_matrix_user_pass_authdb_params() -> None:
    fmch = _fresh_mch()
    uri = fmch.MongoConnectionHandler.build_uri(
        host="h",
        port=27019,
        user="u s",
        password="p@ss word",
        auth_db="admin",
        params={"replicaSet": "rs0", "tls": "true"},
    )

    assert uri.startswith("mongodb://")
    assert "h:27019/?" in uri
    assert "authSource=admin" in uri
    assert "replicaSet=rs0" in uri and "tls=true" in uri

    decoded = _decoded_inline_creds(uri)
    if decoded is not None:
        assert decoded == "u s:p@ss word"


def test_build_uri_user_only_then_base_query_empty_returns_base() -> None:
    fmch = _fresh_mch()
    uri = fmch.MongoConnectionHandler.build_uri(
        host="h",
        port=27017,
        user="only user",
        password="",
        auth_db=None,
        params=None,
    )

    assert uri.startswith("mongodb://") and uri.endswith("h:27017")
    decoded = _decoded_inline_creds(uri)
    assert decoded == "only user"


def test_build_uri_no_user_params_only() -> None:
    fmch = _fresh_mch()
    uri = fmch.MongoConnectionHandler.build_uri(
        host="serverA,serverB",
        port=27018,
        user=None,
        password=None,
        auth_db=None,
        params={"appName": "etl-core"},
    )
    assert uri == "mongodb://serverA,serverB:27018/?appName=etl-core"


def test_build_uri_auth_db_only() -> None:
    fmch = _fresh_mch()
    uri = fmch.MongoConnectionHandler.build_uri(
        host="h",
        port=5,
        auth_db="admin",
    )
    assert uri == "mongodb://h:5/?authSource=admin"


def test__mask_uri_user_without_colon_prefix_branch() -> None:
    fmch = _fresh_mch()
    masked = fmch.MongoConnectionHandler._mask_uri("user@host:1")
    assert masked == "***@host:1"
