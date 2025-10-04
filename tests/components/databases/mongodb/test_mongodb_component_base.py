from __future__ import annotations

from typing import Any, AsyncIterator, Dict, Optional
import pytest

# Module under test
import etl_core.components.databases.mongodb.mongodb as mongodb_mod


class _FakeCreds:
    def __init__(self, payload: Dict[str, Any]) -> None:
        self._payload = payload

    def get_parameter(self, key: str) -> Any:
        return self._payload[key]

    @property
    def decrypted_password(self) -> Optional[str]:
        return self._payload.get("password")


class _FakeContext:
    def __init__(self, payload: Dict[str, Any]) -> None:
        self._payload = payload

    def resolve_active_credentials(self) -> _FakeCreds:
        return _FakeCreds(self._payload)


class _FakeHandler:
    def __init__(self) -> None:
        self.connected = False
        self.closed_calls: list[Dict[str, Any]] = []

    def connect(self, *, uri: str, client_kwargs: Dict[str, Any]) -> None:
        assert uri.startswith("mongodb://")
        assert isinstance(client_kwargs, dict)
        self.connected = True

    def close_pool(self, *, force: bool = False) -> bool:
        self.closed_calls.append({"force": force})
        return force


@pytest.fixture()
def patched_mongodb(monkeypatch: pytest.MonkeyPatch) -> Dict[str, Any]:
    """
    Provide deterministic creds and wrap MongoConnectionHandler so its
    static/class methods (build_uri/_mask_uri) stay available while the
    instance behaves like a fake.
    """
    creds_map = {
        "host": "h",
        "port": 27017,
        "user": "u",
        "password": "p",
        "database": "test_db",
    }

    monkeypatch.setattr(mongodb_mod, "build_mongo_client_kwargs", lambda *_: {})

    monkeypatch.setattr(
        mongodb_mod.MongoDBComponent,
        "_get_credentials",
        lambda self: creds_map,
        raising=True,
    )
    monkeypatch.setattr(
        mongodb_mod.MongoDBComponent,
        "get_resolved_context",
        lambda self: _FakeContext(creds_map),
        raising=True,
    )


    Original = mongodb_mod.MongoConnectionHandler

    class _WrappedFakeHandler(_FakeHandler):  # type: ignore[misc]
        build_uri = staticmethod(Original.build_uri)  # noqa: N805
        if hasattr(Original, "_mask_uri"):
            _mask_uri = staticmethod(getattr(Original, "_mask_uri"))  # noqa: N806

    monkeypatch.setattr(mongodb_mod, "MongoConnectionHandler", _WrappedFakeHandler, raising=True)

    return creds_map


class DummyMongoComp(mongodb_mod.MongoDBComponent):
    # minimal concrete implementation for the abstract base
    name: str
    description: str
    comp_type: str
    entity_name: str
    context_id: str

    async def process_row(self, *_: Any, **__: Any) -> AsyncIterator[Dict[str, Any]]:
        if False:
            yield {}  # pragma: no cover

    async def process_bulk(self, *_: Any, **__: Any) -> AsyncIterator[Dict[str, Any]]:
        if False:
            yield {}  # pragma: no cover

    async def process_bigdata(self, *_: Any, **__: Any) -> AsyncIterator[Dict[str, Any]]:
        if False:
            yield {}  # pragma: no cover


class BadSetupComp(DummyMongoComp):
    def _setup_connection(self) -> None:  # noqa: D401
        return


def _mk_dummy_construct(**overrides: Any) -> DummyMongoComp:
    """
    Build the component WITHOUT validation (SQLModel/Pydantic),
    then run the Mongo-specific setup explicitly.
    """
    base = dict(
        name="c",
        description="",
        comp_type="dummy",
        entity_name="people",
        context_id="ctx",
    )
    base.update(overrides)
    c = DummyMongoComp.model_construct(**base)
    c._setup_connection()
    return c


def _mk_bad_construct(**overrides: Any) -> BadSetupComp:
    base = dict(
        name="bad",
        description="",
        comp_type="dummy",
        entity_name="e",
        context_id="ctx",
    )
    base.update(overrides)
    return BadSetupComp.model_construct(**base)  # no setup on purpose


def test_validator_triggers_setup_and_properties(patched_mongodb: Dict[str, Any]) -> None:
    c = _mk_dummy_construct()
    handler = c.connection_handler
    assert isinstance(handler, _FakeHandler)
    assert handler.connected is True
    assert c.database_name == "test_db"


def test_reuse_existing_handler_and_cleanup_paths(patched_mongodb: Dict[str, Any]) -> None:
    c = _mk_dummy_construct()
    h: _FakeHandler = c.connection_handler

    c.cleanup_after_execution(force=False)
    assert h.closed_calls == [{"force": False}, {"force": True}]

    c2 = _mk_dummy_construct()
    assert isinstance(c2.connection_handler, _FakeHandler)


def test_cleanup_handles_exception_and_leaves_handler(patched_mongodb: Dict[str, Any]) -> None:
    c = _mk_dummy_construct()

    class BoomHandler(_FakeHandler):
        def close_pool(self, *, force: bool = False) -> bool:
            raise RuntimeError("boom")

    c._connection_handler = BoomHandler()
    c.cleanup_after_execution(force=True)
    assert isinstance(c._connection_handler, BoomHandler)


def test_property_errors_when_setup_never_initialized(patched_mongodb: Dict[str, Any]) -> None:
    bad = _mk_bad_construct()
    with pytest.raises(RuntimeError):
        _ = bad.connection_handler
    with pytest.raises(RuntimeError):
        _ = bad.database_name


def test_del_attempts_cleanup(patched_mongodb: Dict[str, Any]) -> None:
    calls: list[bool] = []

    class C(DummyMongoComp):
        def cleanup_after_execution(self, force: bool = False) -> None:
            calls.append(force)

    d = C.model_construct(
        name="d",
        description="",
        comp_type="dummy",
        entity_name="x",
        context_id="ctx",
    )
    d._connection_handler = _FakeHandler()
    C.__del__(d)
    assert calls and calls[-1] is True

def test_setup_connection_logs_and_raises_on_connect_failure(
    patched_mongodb: Dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Force the handler.connect(...) call inside _setup_connection() to raise so we
    cover the exception logging + re-raise path.
    """
    c = _mk_dummy_construct()

    class BoomHandler(_FakeHandler):
        def connect(self, *, uri: str, client_kwargs: Dict[str, Any]) -> None:  # noqa: D401
            raise RuntimeError("connect failed")

    Original = mongodb_mod.MongoConnectionHandler

    class _WrappedBoomHandler(BoomHandler):  # type: ignore[misc]
        build_uri = staticmethod(Original.build_uri)  # noqa: N805
        if hasattr(Original, "_mask_uri"):
            _mask_uri = staticmethod(getattr(Original, "_mask_uri"))  # noqa: N806

    monkeypatch.setattr(mongodb_mod, "MongoConnectionHandler", _WrappedBoomHandler, raising=True)

    fresh = DummyMongoComp.model_construct(
        name="boom",
        description="",
        comp_type="dummy",
        entity_name="people",
        context_id="ctx",
    )

    with pytest.raises(RuntimeError):
        fresh._setup_connection()


def test_cleanup_early_return_when_no_handler(patched_mongodb: Dict[str, Any]) -> None:
    """
    If no handler is present, cleanup_after_execution should return immediately.
    Covers the early 'return' branch.
    """
    c = _mk_bad_construct()
    c.cleanup_after_execution(force=False)


def test_del_swallows_exception_from_cleanup(patched_mongodb: Dict[str, Any]) -> None:
    """
    __del__ wraps cleanup in a try/except: make cleanup raise and ensure no exception
    bubbles out. This covers the except/pass lines in __del__.
    """
    class C(DummyMongoComp):
        def cleanup_after_execution(self, force: bool = False) -> None:
            raise RuntimeError("boom in __del__")

    d = C.model_construct(
        name="deleter",
        description="",
        comp_type="dummy",
        entity_name="x",
        context_id="ctx",
    )
    d._connection_handler = _FakeHandler()

    C.__del__(d)


def test_cleanup_sets_fields_to_none_when_closed(patched_mongodb: Dict[str, Any]) -> None:
    """
    When the handler closes successfully, the component should null out
    _connection_handler/_mongo_uri/_database_name.
    """
    c = _mk_dummy_construct()
    h_before = c._connection_handler
    assert h_before is not None

    c.cleanup_after_execution(force=True)

    assert c._connection_handler is None
    assert c._mongo_uri is None
    assert c._database_name is None

