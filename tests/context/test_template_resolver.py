from __future__ import annotations

from dataclasses import dataclass

import pytest

from etl_core.context.context import Context
from etl_core.context.context_parameter import ContextParameter
from etl_core.context.credentials import Credentials
from etl_core.context.credentials_mapping_context import CredentialsMappingContext
from etl_core.context.environment import Environment
from etl_core.context.secrets.secret_utils import create_secret_provider
from etl_core.context.template_resolver import apply_context_templates_to_component


@dataclass
class _Repo:
    rows: dict[str, Credentials]

    def get_by_id(self, credentials_id: str):
        creds = self.rows.get(credentials_id)
        if creds is None:
            return None
        return creds, credentials_id


class _FakeComponent:
    model_fields = {
        "context_id": None,
        "dsn": None,
        "options": None,
        "scalar": None,
        "routes": None,
    }

    def __init__(self, *, context_id: str, dsn: str, options: dict, scalar: str, ctx):
        self.context_id = context_id
        self.dsn = dsn
        self.options = options
        self.scalar = scalar
        self.routes = {"out": ["${ctx.user}"]}
        self._ctx = ctx

    def get_resolved_context(self):
        return self._ctx


def test_apply_context_templates_uses_environment_specific_credentials() -> None:
    ctx = CredentialsMappingContext(
        name="map",
        environment=Environment.DEV,
        credentials_ids={"DEV": "cred-dev", "PROD": "cred-prod"},
    )
    repo = _Repo(
        rows={
            "cred-dev": Credentials(
                name="dev",
                user="dev_user",
                host="dev-host",
                port=5432,
                database="dev_db",
                password="dev_pw",
                pool_max_size=5,
                pool_timeout_s=2,
            ),
            "cred-prod": Credentials(
                name="prod",
                user="prod_user",
                host="prod-host",
                port=15432,
                database="prod_db",
                password="prod_pw",
                pool_max_size=20,
                pool_timeout_s=5,
            ),
        }
    )

    comp = _FakeComponent(
        context_id="ctx-1",
        dsn="postgres://${ctx.user}:${ctx.password}@${ctx.host}:${ctx.port}/${ctx.database}",
        options={"pool": "${ctx.pool_max_size}"},
        scalar="${ctx.port}",
        ctx=ctx,
    )

    changed = apply_context_templates_to_component(
        comp,
        environment=Environment.PROD,
        creds_handler=repo,  # type: ignore[arg-type]
    )
    assert changed >= 3
    assert comp.dsn == "postgres://prod_user:prod_pw@prod-host:15432/prod_db"
    assert comp.scalar == 15432
    assert comp.options["pool"] == 20
    # structural fields must not be interpolated
    assert comp.routes["out"][0] == "${ctx.user}"


def test_apply_context_templates_raises_on_missing_key() -> None:
    ctx = CredentialsMappingContext(
        name="map",
        environment=Environment.DEV,
        credentials_ids={"DEV": "cred-dev"},
    )
    repo = _Repo(
        rows={
            "cred-dev": Credentials(
                name="dev",
                user="dev_user",
                host="dev-host",
                port=5432,
                database="dev_db",
                password="dev_pw",
            )
        }
    )
    comp = _FakeComponent(
        context_id="ctx-1",
        dsn="prefix-${ctx.unknown}",
        options={},
        scalar="${ctx.port}",
        ctx=ctx,
    )
    with pytest.raises(ValueError, match="Unknown context placeholder key"):
        apply_context_templates_to_component(
            comp,
            environment=Environment.DEV,
            creds_handler=repo,  # type: ignore[arg-type]
        )


def test_apply_context_templates_reads_secure_context_parameter() -> None:
    secret_store = create_secret_provider()
    secret_key = "ctx-secure/api_key"
    secret_store.set(secret_key, "top-secret-token")

    ctx = Context(
        name="generic",
        environment=Environment.DEV,
        parameters={
            "api_key": ContextParameter(
                id=1,
                key="api_key",
                value="",
                type="string",
                is_secure=True,
            )
        },
    )

    comp = _FakeComponent(
        context_id="ctx-secure",
        dsn="Bearer ${ctx.api_key}",
        options={},
        scalar="${ctx.api_key}",
        ctx=ctx,
    )

    try:
        apply_context_templates_to_component(comp, environment=Environment.DEV)
        assert comp.dsn == "Bearer top-secret-token"
        assert comp.scalar == "top-secret-token"
    finally:
        try:
            secret_store.delete(secret_key)
        except Exception:  # noqa: BLE001
            pass
