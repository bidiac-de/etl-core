from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import HTTPException, Response
from sqlalchemy.exc import IntegrityError

import etl_core.api.routers.contexts as C


class DummyAdapter:
    def __init__(self) -> None:
        self.deleted = False

    def delete_from_store(self) -> None:
        self.deleted = True


def test_create_context_provider_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    # Force SecureContextAdapter to raise to hit the 400 branch
    monkeypatch.setattr(
        C,
        "SecureContextAdapter",
        lambda **kw: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    # NOTE: environment must be a STRING ("DEV"|"TEST"|"PROD"), not the enum
    ctx = C.Context(environment="DEV", parameters={}, name="x")
    req = C.ContextCreateRequest(context=ctx)

    with pytest.raises(HTTPException) as e:
        C.create_context_provider(req, default_provider=Mock(), ctx_handler=Mock())
    assert e.value.status_code == 400


def test_create_credentials_provider_with_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BadAdapter:
        def bootstrap_to_store(self) -> SimpleNamespace:
            return SimpleNamespace(errors={"pw": "bad"})

    monkeypatch.setattr(C, "SecureContextAdapter", lambda **kw: BadAdapter())
    creds = C.Credentials(
        name="c",
        user="u",
        host="h",
        port=1,
        database="db",
        password="pw",
    )
    req = C.CredentialsCreateRequest(credentials=creds)

    with pytest.raises(HTTPException) as e:
        C.create_credentials_provider(
            req, default_provider=Mock(), creds_handler=Mock()
        )
    assert e.value.status_code == 400


def test_create_credentials_mapping_context_success_and_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx_handler = Mock()
    creds_handler = Mock()
    creds_handler.get_by_id.side_effect = lambda cid: (
        SimpleNamespace() if cid == "ok" else None
    )

    # SUCCESS: environment must be STRING
    cmc_ok = C.CredentialsMappingContext(
        name="m",
        environment="DEV",
        credentials_ids={"DEV": "ok"},
    )
    req_ok = C.CredentialsMappingContextCreateRequest(context=cmc_ok)
    out = C.create_credentials_mapping_context(
        req_ok, ctx_handler=ctx_handler, creds_handler=creds_handler
    )
    assert out.kind == "context"

    # MISSING: unknown credentials id → 400
    cmc_bad = C.CredentialsMappingContext(
        name="m",
        environment="DEV",
        credentials_ids={"DEV": "missing"},
    )
    req_bad = C.CredentialsMappingContextCreateRequest(context=cmc_bad)
    with pytest.raises(HTTPException) as e:
        C.create_credentials_mapping_context(
            req_bad, ctx_handler=ctx_handler, creds_handler=creds_handler
        )
    assert e.value.status_code == 400


def test_list_providers_dedupes() -> None:
    ctx_handler = Mock()
    creds_handler = Mock()
    row = SimpleNamespace(id="x", name="n", environment="DEV")
    ctx_handler.list_all.return_value = [row, row]  # duplicate id
    creds_handler.list_all.return_value = []

    out = C.list_providers(ctx_handler=ctx_handler, creds_handler=creds_handler)
    assert len(out) == 1
    assert out[0].id == "x"


def test_get_provider_context_fallback() -> None:
    ctx_handler = Mock()
    creds_handler = Mock()

    ctx_obj = SimpleNamespace(environment="DEV", parameters={}, credentials_ids={})
    ctx_handler.get_by_id.return_value = (ctx_obj, "id1")
    ctx_handler.list_all.return_value = [
        SimpleNamespace(id="id1", name="n", environment="DEV")
    ]

    out = C.get_provider("id1", ctx_handler=ctx_handler, creds_handler=creds_handler)
    assert out.kind == "context"
    assert out.name == "n"
    assert out.environment == "DEV"


def test_get_provider_credentials_and_not_found() -> None:
    ctx_handler = Mock()
    ctx_handler.get_by_id.return_value = None

    creds = SimpleNamespace(
        name="c",
        user="u",
        host="h",
        port=1,
        database="db",
        pool_max_size=None,
        pool_timeout_s=None,
        password=None,
    )
    creds_handler = Mock()
    creds_handler.get_by_id.return_value = (creds, "idc")

    out = C.get_provider("idc", ctx_handler=ctx_handler, creds_handler=creds_handler)
    assert out.kind == "credentials"
    assert out.name == "c"

    creds_handler.get_by_id.return_value = None
    with pytest.raises(HTTPException) as e:
        C.get_provider("missing", ctx_handler=ctx_handler, creds_handler=creds_handler)
    assert e.value.status_code == 404


def test_delete_provider_with_adapter_success(monkeypatch: pytest.MonkeyPatch) -> None:
    ctx_handler = Mock(delete_by_id=Mock(return_value=True))
    creds_handler = Mock(delete_by_id=Mock(return_value=False))
    dummy = DummyAdapter()

    monkeypatch.setattr(C.ContextRegistry, "resolve", lambda _id: dummy)
    monkeypatch.setattr(C.ContextRegistry, "unregister", lambda _id: None)

    resp = C.delete_provider("x", ctx_handler=ctx_handler, creds_handler=creds_handler)
    assert isinstance(resp, Response)
    assert resp.status_code == 200
    assert dummy.deleted is True


def test_delete_provider_404_when_nothing_deleted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx_handler = Mock(delete_by_id=Mock(return_value=False))
    creds_handler = Mock(delete_by_id=Mock(return_value=False))
    monkeypatch.setattr(C.ContextRegistry, "resolve", lambda _id: None)

    with pytest.raises(HTTPException) as e:
        C.delete_provider(
            "missing", ctx_handler=ctx_handler, creds_handler=creds_handler
        )
    assert e.value.status_code == 404


def test_delete_provider_409_on_integrity_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # One of the handlers raises IntegrityError → 409
    ctx_handler = Mock(delete_by_id=Mock(side_effect=IntegrityError("stmt", {}, None)))
    creds_handler = Mock(delete_by_id=Mock(return_value=False))
    monkeypatch.setattr(C.ContextRegistry, "resolve", lambda _id: None)

    with pytest.raises(HTTPException) as e:
        C.delete_provider("y", ctx_handler=ctx_handler, creds_handler=creds_handler)
    assert e.value.status_code == 409


def test_delete_provider_500_on_unexpected_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx_handler = Mock(delete_by_id=Mock(side_effect=RuntimeError("x")))
    creds_handler = Mock(delete_by_id=Mock(return_value=False))
    monkeypatch.setattr(C.ContextRegistry, "resolve", lambda _id: None)

    with pytest.raises(HTTPException) as e:
        C.delete_provider("y", ctx_handler=ctx_handler, creds_handler=creds_handler)
    assert e.value.status_code == 500
