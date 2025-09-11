from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from typer.testing import CliRunner

from etl_core.api.cli.main import app


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _write_json(tmp_path, name: str, payload: dict) -> str:
    p = tmp_path / name
    p.write_text(json.dumps(payload), encoding="utf-8")
    return str(p)


def test_create_context_remote(runner: CliRunner, tmp_path) -> None:
    cfg = {"name": "ctx1", "environment": "TEST", "parameters": {}}
    path = _write_json(tmp_path, "context.json", cfg)

    with patch("etl_core.api.cli.adapters.requests") as mreq:
        resp = Mock()
        resp.json.return_value = {"id": "prov-ctx-1"}
        resp.raise_for_status.return_value = None
        mreq.post.return_value = resp

        res = runner.invoke(
            app,
            [
                "contexts",
                "create-context",
                path,
                "--remote",
                "--base-url",
                "http://localhost:8000",
            ],
        )
        assert res.exit_code == 0
        assert "prov-ctx-1" in res.stdout
        mreq.post.assert_called_once_with(
            "http://localhost:8000/contexts/context",
            json={"context": cfg, "keyring_service": None},
            timeout=30,
        )


def test_create_credentials_remote(runner: CliRunner, tmp_path) -> None:
    creds = {
        "name": "db_creds",
        "user": "u",
        "host": "h",
        "port": 5432,
        "database": "d",
        "password": "secret",  # server handles secrecy; client just forwards JSON
        "pool_max_size": 5,
        "pool_timeout_s": 10,
    }
    path = _write_json(tmp_path, "creds.json", creds)

    with patch("etl_core.api.cli.adapters.requests") as mreq:
        resp = Mock()
        resp.json.return_value = {"id": "prov-creds-1"}
        resp.raise_for_status.return_value = None
        mreq.post.return_value = resp

        res = runner.invoke(
            app,
            [
                "contexts",
                "create-credentials",
                path,
                "--remote",
                "--base-url",
                "http://localhost:8000",
            ],
        )
        assert res.exit_code == 0
        assert "prov-creds-1" in res.stdout
        mreq.post.assert_called_once_with(
            "http://localhost:8000/contexts/credentials",
            json={"credentials": creds, "keyring_service": None},
            timeout=30,
        )


def test_create_context_mapping_remote(runner: CliRunner, tmp_path) -> None:
    mapping = {
        "name": "db_mapping",
        "environment": "TEST",
        "credentials_ids": {"TEST": "prov-creds-1"},
    }
    path = _write_json(tmp_path, "mapping.json", mapping)

    with patch("etl_core.api.cli.adapters.requests") as mreq:
        resp = Mock()
        resp.json.return_value = {"id": "prov-map-1"}
        resp.raise_for_status.return_value = None
        mreq.post.return_value = resp

        res = runner.invoke(
            app,
            [
                "contexts",
                "create-context-mapping",
                path,
                "--remote",
                "--base-url",
                "http://localhost:8000",
            ],
        )
        assert res.exit_code == 0
        assert "prov-map-1" in res.stdout
        mreq.post.assert_called_once_with(
            "http://localhost:8000/contexts/context-mapping",
            json={"context": mapping},
            timeout=30,
        )


def test_get_provider_remote(runner: CliRunner) -> None:
    with patch("etl_core.api.cli.adapters.requests") as mreq:
        resp = Mock()
        resp.json.return_value = {
            "id": "prov-ctx-1",
            "kind": "context",
            "environment": "TEST",
        }
        resp.raise_for_status.return_value = None
        mreq.get.return_value = resp

        res = runner.invoke(
            app,
            [
                "contexts",
                "get",
                "prov-ctx-1",
                "--remote",
                "--base-url",
                "http://localhost:8000",
            ],
        )
        assert res.exit_code == 0
        assert '"prov-ctx-1"' in res.stdout
        mreq.get.assert_called_once_with(
            "http://localhost:8000/contexts/prov-ctx-1", timeout=30
        )


def test_list_providers_remote(runner: CliRunner) -> None:
    with patch("etl_core.api.cli.adapters.requests") as mreq:
        resp = Mock()
        resp.json.return_value = [
            {"id": "prov-ctx-1", "kind": "context"},
            {"id": "prov-creds-1", "kind": "credentials"},
        ]
        resp.raise_for_status.return_value = None
        mreq.get.return_value = resp

        res = runner.invoke(
            app,
            ["contexts", "list", "--remote", "--base-url", "http://localhost:8000"],
        )
        assert res.exit_code == 0
        assert "prov-ctx-1" in res.stdout and "prov-creds-1" in res.stdout
        mreq.get.assert_called_once_with("http://localhost:8000/contexts/", timeout=30)


def test_delete_provider_remote(runner: CliRunner) -> None:
    with patch("etl_core.api.cli.adapters.requests") as mreq:
        resp = Mock()
        resp.raise_for_status.return_value = None
        mreq.delete.return_value = resp

        res = runner.invoke(
            app,
            [
                "contexts",
                "delete",
                "prov-ctx-1",
                "--remote",
                "--base-url",
                "http://localhost:8000",
            ],
        )
        assert res.exit_code == 0
        assert "Deleted provider 'prov-ctx-1'" in res.stdout
        mreq.delete.assert_called_once_with(
            "http://localhost:8000/contexts/prov-ctx-1", timeout=30
        )


def test_list_providers_local(runner: CliRunner) -> None:
    ctx_rows = [
        SimpleNamespace(provider_id="prov-ctx-1", name="ctx1", environment="TEST"),
        SimpleNamespace(provider_id="prov-ctx-2", name="ctx2", environment="DEV"),
    ]
    cred_rows = [
        SimpleNamespace(provider_id="prov-creds-1", name="creds1"),
    ]

    with (
        patch("etl_core.api.cli.adapters.ContextHandler") as CtxH,
        patch("etl_core.api.cli.adapters.CredentialsHandler") as CredH,
    ):
        CtxH.return_value.list_all.return_value = ctx_rows
        CredH.return_value.list_all.return_value = cred_rows

        res = runner.invoke(app, ["contexts", "list"])
        assert res.exit_code == 0
        out = res.stdout
        assert "prov-ctx-1" in out and "prov-ctx-2" in out and "prov-creds-1" in out


def test_get_provider_local_context_branch(runner: CliRunner) -> None:
    with (
        patch("etl_core.api.cli.adapters.ContextHandler") as CtxH,
        patch("etl_core.api.cli.adapters.CredentialsHandler") as CredH,
    ):
        CtxH.return_value.get_by_provider_id.return_value = SimpleNamespace(
            environment="TEST"
        )
        CredH.return_value.get_by_provider_id.return_value = None

        res = runner.invoke(app, ["contexts", "get", "prov-ctx-1"])
        assert res.exit_code == 0
        assert '"kind": "context"' in res.stdout
        CtxH.return_value.get_by_provider_id.assert_called_once_with("prov-ctx-1")


def test_get_provider_local_credentials_branch(runner: CliRunner) -> None:
    with (
        patch("etl_core.api.cli.adapters.ContextHandler") as CtxH,
        patch("etl_core.api.cli.adapters.CredentialsHandler") as CredH,
    ):
        CtxH.return_value.get_by_provider_id.return_value = None
        CredH.return_value.get_by_provider_id.return_value = SimpleNamespace()

        res = runner.invoke(app, ["contexts", "get", "prov-creds-1"])
        assert res.exit_code == 0
        assert '"kind": "credentials"' in res.stdout
        CredH.return_value.get_by_provider_id.assert_called_once_with("prov-creds-1")


def test_get_provider_local_not_found(runner: CliRunner) -> None:
    with (
        patch("etl_core.api.cli.adapters.ContextHandler") as CtxH,
        patch("etl_core.api.cli.adapters.CredentialsHandler") as CredH,
    ):
        CtxH.return_value.get_by_provider_id.return_value = None
        CredH.return_value.get_by_provider_id.return_value = None

        res = runner.invoke(app, ["contexts", "get", "missing-id"])
        assert res.exit_code != 0  # Typer exits with non-zero when exception bubbles


def test_delete_provider_local(runner: CliRunner) -> None:
    with (
        patch("etl_core.api.cli.adapters.ContextHandler") as CtxH,
        patch("etl_core.api.cli.adapters.CredentialsHandler") as CredH,
    ):
        res = runner.invoke(app, ["contexts", "delete", "prov-any"])
        assert res.exit_code == 0
        # Best-effort deletion: both handlers are attempted
        assert CtxH.return_value.delete_by_provider_id.called
        assert CredH.return_value.delete_by_provider_id.called
