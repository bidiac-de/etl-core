from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from etl_core.api.routers import contexts as C
from etl_core.context.environment import Environment
from etl_core.context.credentials import Credentials


class FakeSecretProvider:
    pass


class FailProvider:
    def __init__(self):
        raise RuntimeError("no provider")


class FakeAdapter:
    def __init__(self, provider_id, secret_store, context=None, credentials=None):  # noqa: D401
        self.provider_id = provider_id
        self.secret_store = secret_store
        self.context = context
        self.credentials = credentials
        self._deleted = False

    def bootstrap_to_store(self):
        return SimpleNamespace(errors={})

    def delete_from_store(self):
        self._deleted = True


class FakeContextRow:
    def __init__(self, id_: str, name: str = "n", env: str = "DEV"):
        self.id = id_
        self.name = name
        self.environment = env
        self.non_secure_params = {}
        self.secure_param_keys = []


class FakeCredsRow:
    def __init__(self, id_: str, name: str = "c"):
        self.id = id_
        self.name = name


class FakeContextHandler:
    def __init__(self):
        self.rows = {}

    def upsert(self, context_id, name, environment, non_secure_params, secure_param_keys):
        self.rows[context_id] = FakeContextRow(context_id, name, environment)
        return context_id

    def upsert_credentials_mapping_context(self, context_id, name, environment, mapping_env_to_credentials_id):
        self.rows[context_id] = FakeContextRow(context_id, name, environment)
        return context_id

    def list_all(self):
        return list(self.rows.values())

    def get_by_id(self, id_):
        row = self.rows.get(id_)
        # Return environment as a plain string so str(env) == 'DEV' as router expects
        return (SimpleNamespace(environment=row.environment), id_) if row else None

    def delete_by_id(self, id_):
        self.rows.pop(id_, None)


class FakeCredsHandler:
    def __init__(self):
        self.rows = {}

    def upsert(self, creds, credentials_id):
        self.rows[credentials_id] = FakeCredsRow(credentials_id)
        return credentials_id

    def list_all(self):
        return list(self.rows.values())

    def get_by_id(self, id_):
        return self.rows.get(id_)

    def delete_by_id(self, id_):
        self.rows.pop(id_, None)


def test_get_secret_provider_errors_to_http(monkeypatch):
    monkeypatch.setattr(C, "create_secret_provider", lambda: (_ for _ in ()).throw(RuntimeError("x")))
    with pytest.raises(HTTPException) as ei:
        C.get_secret_provider()
    assert ei.value.status_code == 500


def test_create_credentials_and_context_and_list_get_delete(monkeypatch):
    # patch adapter and secret provider
    monkeypatch.setattr(C, "SecureContextAdapter", FakeAdapter)
    monkeypatch.setattr(C, "create_secret_provider", lambda: FakeSecretProvider())

    ctx_handler = FakeContextHandler()
    creds_handler = FakeCredsHandler()

    # create credentials (dict to match router's model type path)
    creds_payload = {
        "name": "conn",
        "user": "u",
        "host": "h",
        "port": 5432,
        "database": "db",
        "password": None,
    }
    body_creds = C.CredentialsCreateRequest(credentials=creds_payload)
    resp_creds = C.create_credentials_provider(body_creds, default_provider=C.get_secret_provider(), creds_handler=creds_handler)
    assert resp_creds.kind == "credentials" and resp_creds.id in creds_handler.rows

    # create context
    ctx = C.Context(environment=Environment.DEV, parameters={}, name="n")
    body_ctx = C.ContextCreateRequest(context=ctx)
    resp_ctx = C.create_context_provider(body_ctx, default_provider=C.get_secret_provider(), ctx_handler=ctx_handler)
    assert resp_ctx.kind == "context" and resp_ctx.id in ctx_handler.rows

    # list providers contains both
    providers = C.list_providers(ctx_handler=ctx_handler, creds_handler=creds_handler)
    kinds = {p.kind for p in providers}
    assert kinds == {"context", "credentials"}

    # get provider for context id
    info = C.get_provider(resp_ctx.id, ctx_handler=ctx_handler, creds_handler=creds_handler)
    assert info.kind == "context"

    # delete should not error
    out = C.delete_provider(resp_ctx.id, creds_handler=creds_handler, ctx_handler=ctx_handler)
    assert out.status_code == 204
