from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import Mock

import pytest
import requests

import etl_core.api.cli.adapters as adapters
from etl_core.persistence.errors import PersistNotFoundError


def _resp(
    *,
    status: int,
    method: Optional[str] = "GET",
    url: Optional[str] = "http://user:pass@host.tld/p?q=1#frag",
    reason: str = "OK",
) -> requests.Response:
    """Build a minimal Response-like object sufficient for _raise_for_status."""
    req = SimpleNamespace(method=method, url=url)
    r = SimpleNamespace(
        status_code=status,
        reason=reason,
        request=req,
        json=lambda: {"ok": True},
        raise_for_status=lambda: None,
    )
    return r  # type: ignore[return-value]


def test_api_base_url_none_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ETL_API_BASE_URL", raising=False)
    assert adapters.api_base_url() is None


def test_api_base_url_trailing_slash_trim(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ETL_API_BASE_URL", "http://example.local/api/")
    assert adapters.api_base_url() == "http://example.local/api"


def test__dedupe_preserves_order_and_unique_by_id() -> None:
    items: List[Dict[str, Any]] = [
        {"id": 1, "v": "a"},
        {"id": "1", "v": "duplicate-str"},
        {"id": 2, "v": "b"},
        {"id": 1, "v": "duplicate-int"},
        {"id": 3, "v": "c"},
        {"id": "3", "v": "duplicate-3"},
    ]
    out = adapters._dedupe(items)
    assert [x["id"] for x in out] == [1, 2, 3]
    assert out[0]["v"] == "a"
    assert out[1]["v"] == "b"
    assert out[2]["v"] == "c"


def test__sanitize_url_masks_credentials_and_drops_query_fragment() -> None:
    raw = "https://user:pw@server.example:8443/a/b?x=1&y=2#sec"
    sanitized = adapters._RestBase._sanitize_url(raw)
    assert "***@" in sanitized
    assert "user:pw@" not in sanitized
    assert "?" not in sanitized and "#" not in sanitized
    assert sanitized.startswith("https://***@server.example:8443/a/b")


def test__sanitize_url_unknown_and_invalid(monkeypatch: pytest.MonkeyPatch) -> None:
    assert adapters._RestBase._sanitize_url(None) == "<unknown>"

    sentinel = "force-invalid"
    original = adapters.urlsplit

    def _boom(value: str):  # type: ignore[override]
        if value == sentinel:
            raise ValueError("bad url")
        return original(value)

    monkeypatch.setattr(adapters, "urlsplit", _boom)
    assert adapters._RestBase._sanitize_url(sentinel) == "<invalid-url>"


def test__raise_for_status_404_raises_persist_not_found() -> None:
    r = _resp(status=404)
    base = adapters._RestBase("http://api")
    with pytest.raises(PersistNotFoundError) as exc:
        base._raise_for_status(r)  # type: ignore[arg-type]
    msg = str(exc.value)
    assert "Resource not found" in msg
    assert "***@" in msg
    assert "GET" in msg
    assert "?" not in msg and "#" not in msg


def test__raise_for_status_other_http_error_rewraps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    r = _resp(status=400, reason="Bad Request")
    err = requests.HTTPError("orig")
    r.raise_for_status = Mock(side_effect=err)  # type: ignore[attr-defined]

    base = adapters._RestBase("http://api")
    with pytest.raises(requests.HTTPError) as exc:
        base._raise_for_status(r)  # type: ignore[arg-type]

    msg = str(exc.value)
    assert "400 Bad Request" in msg
    assert "for GET" in msg
    assert "***@" in msg
    assert "?" not in msg and "#" not in msg


def test__raise_for_status_ok_and_missing_request_url_is_safe() -> None:
    r = _resp(status=200, url=None)
    base = adapters._RestBase("http://api")
    base._raise_for_status(r)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("client_cls", "http_method", "call"),
    [
        (adapters.RemoteJobsClient, "get", lambda c: c.get("jid")),
        (adapters.RemoteJobsClient, "post", lambda c: c.create({"x": 1})),
        (adapters.RemoteJobsClient, "put", lambda c: c.update("jid", {"x": 2})),
        (adapters.RemoteJobsClient, "delete", lambda c: c.delete("jid")),
        (adapters.RemoteContextsClient, "get", lambda c: c.get_provider("pid")),
        (adapters.RemoteContextsClient, "delete", lambda c: c.delete_provider("pid")),
        (adapters.RemoteContextsClient, "get", lambda c: c.list_providers()),
    ],
)
def test_remote_clients_delegate_and_use_raise_for_status(
    monkeypatch: pytest.MonkeyPatch,
    client_cls: Any,
    http_method: str,
    call,
) -> None:
    resp = _resp(status=200)
    session = Mock()
    setattr(session, http_method, Mock(return_value=resp))
    session.post.return_value = resp
    session.put.return_value = resp
    session.delete.return_value = resp

    monkeypatch.setattr(adapters, "requests", Mock(Session=lambda: session))

    client = client_cls("http://api")
    out = call(client)
    if http_method in {"get", "post", "put"}:
        assert out == {"ok": True}
