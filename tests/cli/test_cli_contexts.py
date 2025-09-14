from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import patch

from typer.testing import CliRunner

from etl_core.api.cli.cli_app import app


def _write_json(tmp_path, name: str, payload: dict) -> str:
    p = tmp_path / name
    p.write_text(json.dumps(payload), encoding="utf-8")
    return str(p)


def test_create_context_local(tmp_path) -> None:
    cfg = {"name": "ctx1", "environment": "TEST", "parameters": {}}
    path = _write_json(tmp_path, "context.json", cfg)

    with patch("etl_core.api.cli.adapters._ch_singleton") as CtxH:
        runner = CliRunner()
        res = runner.invoke(app, ["contexts", "create-context", path])
        assert res.exit_code == 0

        body = json.loads(res.stdout)
        assert isinstance(body, dict)
        assert body.get("kind") == "context"
        assert body.get("environment") == "TEST"
        assert isinstance(body.get("id"), str) and body["id"]

        assert CtxH.return_value.upsert.call_count == 1
        _, kwargs = CtxH.return_value.upsert.call_args
        assert isinstance(kwargs["context_id"], str) and kwargs["context_id"]
        assert kwargs["name"] == "ctx1"
        assert kwargs["environment"] == "TEST"
        assert kwargs["non_secure_params"] == {}
        assert kwargs["secure_param_keys"] == []


def test_create_credentials_local(tmp_path) -> None:
    creds = {
        "name": "db_creds",
        "user": "u",
        "host": "h",
        "port": 5432,
        "database": "d",
        "password": "secret",
        "pool_max_size": 5,
        "pool_timeout_s": 10,
    }
    path = _write_json(tmp_path, "creds.json", creds)

    with (patch("etl_core.api.cli.adapters._crh_singleton") as CredH,):
        runner = CliRunner()
        res = runner.invoke(app, ["contexts", "create-credentials", path])
        assert res.exit_code == 0

        body = json.loads(res.stdout)
        assert isinstance(body, dict)
        assert body.get("kind") == "credentials"
        assert "environment" in body
        assert isinstance(body.get("id"), str) and body["id"]

        assert CredH.return_value.upsert.call_count == 1
        args, kwargs = CredH.return_value.upsert.call_args
        sent_creds = args[0]
        assert sent_creds.name == "db_creds"
        assert sent_creds.user == "u"
        assert sent_creds.host == "h"
        assert sent_creds.port == 5432
        assert sent_creds.database == "d"
        assert sent_creds.password is None
        assert isinstance(kwargs["credentials_id"], str) and kwargs["credentials_id"]


def test_create_context_mapping_local(tmp_path) -> None:
    mapping = {
        "name": "db_mapping",
        "environment": "TEST",
        "credentials_ids": {"TEST": "prov-creds-1"},
    }
    path = _write_json(tmp_path, "mapping.json", mapping)

    with (
        patch("etl_core.api.cli.adapters._ch_singleton") as CtxH,
        patch("etl_core.api.cli.adapters._crh_singleton") as CredH,
    ):
        CredH.return_value.get_by_id.return_value = SimpleNamespace(id="prov-creds-1")

        runner = CliRunner()
        res = runner.invoke(app, ["contexts", "create-context-mapping", path])
        assert res.exit_code == 0

        body = json.loads(res.stdout)
        assert isinstance(body, dict)
        assert isinstance(body.get("id"), str) and body["id"]
        assert body.get("kind") == "context"
        assert body.get("environment") == "TEST"
        assert body.get("parameters_registered") == 1

        assert CtxH.return_value.upsert_credentials_mapping_context.call_count == 1
        _, kwargs = CtxH.return_value.upsert_credentials_mapping_context.call_args
        assert isinstance(kwargs["context_id"], str) and kwargs["context_id"]
        assert kwargs["name"] == "db_mapping"
        assert kwargs["environment"] == "TEST"
        assert kwargs["mapping_env_to_credentials_id"] == {"TEST": "prov-creds-1"}


def test_list_providers_local() -> None:
    ctx_rows = [
        SimpleNamespace(id="prov-ctx-1", name="ctx1", environment="TEST"),
        SimpleNamespace(id="prov-ctx-2", name="ctx2", environment="DEV"),
    ]
    cred_rows = [
        SimpleNamespace(id="prov-creds-1", name="creds1"),
    ]

    with (
        patch("etl_core.api.cli.adapters._ch_singleton") as CtxH,
        patch("etl_core.api.cli.adapters._crh_singleton") as CredH,
    ):
        CtxH.return_value.list_all.return_value = ctx_rows
        CredH.return_value.list_all.return_value = cred_rows

        runner = CliRunner()
        res = runner.invoke(app, ["contexts", "list"])
        assert res.exit_code == 0
        out = res.stdout
        assert "prov-ctx-1" in out and "prov-ctx-2" in out and "prov-creds-1" in out


def test_get_provider_local_context_branch() -> None:
    with (
        patch("etl_core.api.cli.adapters._ch_singleton") as CtxH,
        patch("etl_core.api.cli.adapters._crh_singleton") as CredH,
    ):
        CtxH.return_value.get_by_id.return_value = (
            SimpleNamespace(environment="TEST"),
            "prov-ctx-1",
        )
        CredH.return_value.get_by_id.return_value = None

        runner = CliRunner()
        res = runner.invoke(app, ["contexts", "get", "prov-ctx-1"])
        assert res.exit_code == 0
        assert '"kind": "context"' in res.stdout
        CtxH.return_value.get_by_id.assert_called_once_with("prov-ctx-1")


def test_get_provider_local_credentials_branch() -> None:
    with (
        patch("etl_core.api.cli.adapters._ch_singleton") as CtxH,
        patch("etl_core.api.cli.adapters._crh_singleton") as CredH,
    ):
        CtxH.return_value.get_by_id.return_value = None
        CredH.return_value.get_by_id.return_value = (
            SimpleNamespace(name="creds"),
            "prov-creds-1",
        )

        runner = CliRunner()
        res = runner.invoke(app, ["contexts", "get", "prov-creds-1"])
        assert res.exit_code == 0
        assert '"kind": "credentials"' in res.stdout
        CredH.return_value.get_by_id.assert_called_once_with("prov-creds-1")


def test_get_provider_local_not_found() -> None:
    with (
        patch("etl_core.api.cli.adapters._ch_singleton") as CtxH,
        patch("etl_core.api.cli.adapters._crh_singleton") as CredH,
    ):
        CtxH.return_value.get_by_id.return_value = None
        CredH.return_value.get_by_id.return_value = None

        runner = CliRunner()
        res = runner.invoke(app, ["contexts", "get", "missing-id"])
        assert res.exit_code != 0


def test_delete_provider_local() -> None:
    with (
        patch("etl_core.api.cli.adapters._ch_singleton") as CtxH,
        patch("etl_core.api.cli.adapters._crh_singleton") as CredH,
    ):
        runner = CliRunner()
        res = runner.invoke(app, ["contexts", "delete", "prov-any"])
        assert res.exit_code == 0
        assert CtxH.return_value.delete_by_id.called
        assert CredH.return_value.delete_by_id.called
