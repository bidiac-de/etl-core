from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict
from unittest.mock import Mock

import pytest

import etl_core.api.cli.adapters as adapters


def _resp200(payload: Dict[str, Any] | None = None) -> Any:
    payload = {"ok": True} if payload is None else payload
    req = SimpleNamespace(method="GET", url="http://h/p")
    return SimpleNamespace(
        status_code=200, reason="OK", request=req, json=lambda: payload
    )


def test_local_execution_list_executions_with_filters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = Mock()
    records.list_executions.return_value = ([], 0)
    monkeypatch.setattr(adapters, "_erh_singleton", lambda: records)
    monkeypatch.setattr(adapters, "_jh_singleton", lambda: Mock())
    monkeypatch.setattr(adapters, "_eh_singleton", lambda: Mock())

    client = adapters.LocalExecutionClient()
    sa = "2024-01-02T03:04:05"
    sb = "2024-02-03T04:05:06"
    client.list_executions(
        job_id="jid",
        status="ok",
        environment="DEV",
        started_after=sa,
        started_before=sb,
        sort_by="started_at",
        order="asc",
        limit=10,
        offset=5,
    )

    assert records.list_executions.called
    kwargs = records.list_executions.call_args.kwargs
    assert kwargs["job_id"] == "jid"
    assert kwargs["status"] == "ok"
    assert kwargs["environment"] == "DEV"
    assert kwargs["sort_by"] == "started_at"
    assert kwargs["order"] == "asc"
    assert kwargs["limit"] == 10
    assert kwargs["offset"] == 5
    assert hasattr(kwargs["started_after"], "isoformat")
    assert hasattr(kwargs["started_before"], "isoformat")
    assert kwargs["started_after"].isoformat().startswith("2024-01-02T03:04:05")
    assert kwargs["started_before"].isoformat().startswith("2024-02-03T04:05:06")


def test__non_secure_params_filters_secure_values() -> None:
    ctx = SimpleNamespace(
        parameters={
            "plain": SimpleNamespace(value="v1", is_secure=False),
            "secret": SimpleNamespace(value="v2", is_secure=True),
        }
    )
    out = adapters.LocalContextsClient._non_secure_params(ctx)  # type: ignore[arg-type]
    assert out == {"plain": "v1"}


def test_remote_execution_start_with_environment_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = Mock()
    session.post.return_value = _resp200({"status": "ok"})
    monkeypatch.setattr(adapters, "requests", Mock(Session=lambda: session))

    client = adapters.RemoteExecutionClient("http://api")
    client._raise_for_status = lambda r: None  # type: ignore[method-assign]
    env = SimpleNamespace(value="PROD")
    out = client.start("jid-123", environment=env)  # type: ignore[arg-type]

    assert out == {"status": "ok"}
    assert session.post.called
    (url,) = session.post.call_args.args
    kwargs = session.post.call_args.kwargs
    assert url.endswith("/execution/jid-123")
    assert kwargs["json"] == {"environment": "PROD"}


def test_remote_execution_list_executions_query_building(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = Mock()
    session.get.return_value = _resp200({"data": []})
    monkeypatch.setattr(adapters, "requests", Mock(Session=lambda: session))

    client = adapters.RemoteExecutionClient("http://api")
    client._raise_for_status = lambda r: None  # type: ignore[method-assign]
    client.list_executions(
        job_id="J1",
        status="done",
        environment="TEST",
        started_after="2024-01-01T00:00:00",
        started_before="2024-01-31T23:59:59",
        sort_by="started_at",
        order="desc",
        limit=99,
        offset=7,
    )

    assert session.get.called
    (url,) = session.get.call_args.args
    assert url.startswith("http://api/execution/executions?")
    for part in [
        "job_id=J1",
        "status=done",
        "environment=TEST",
        "started_after=2024-01-01T00%3A00%3A00",
        "started_before=2024-01-31T23%3A59%3A59",
        "sort_by=started_at",
        "order=desc",
        "limit=99",
        "offset=7",
    ]:
        assert part in url


def test_remote_jobs_list_brief_and_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    session = Mock()
    session.get.return_value = _resp200({"ok": True})
    monkeypatch.setattr(adapters, "requests", Mock(Session=lambda: session))

    jobs = adapters.RemoteJobsClient("http://api")
    jobs._raise_for_status = lambda r: None  # type: ignore[method-assign]
    out = jobs.list_brief()
    assert out == {"ok": True}
    (url_jobs,) = session.get.call_args.args
    assert url_jobs.endswith("/jobs/")

    session.get.reset_mock()
    session.get.return_value = _resp200([{"id": "a1"}])
    exec_client = adapters.RemoteExecutionClient("http://api")
    exec_client._raise_for_status = lambda r: None  # type: ignore[method-assign]
    out2 = exec_client.attempts("e-1")
    assert out2 == [{"id": "a1"}]
    (url_attempts,) = session.get.call_args.args
    assert url_attempts.endswith("/execution/executions/e-1/attempts")


def test_remote_contexts_posts_payloads(monkeypatch: pytest.MonkeyPatch) -> None:
    session = Mock()
    session.post.return_value = _resp200({"ok": True})
    monkeypatch.setattr(adapters, "requests", Mock(Session=lambda: session))

    client = adapters.RemoteContextsClient("http://api")
    client._raise_for_status = lambda r: None  # type: ignore[method-assign]

    ctx = {"name": "ctx", "environment": "DEV", "parameters": {}}
    out1 = client.create_context(ctx, keyring_service="svc")
    assert out1 == {"ok": True}
    _, kwargs1 = session.post.call_args
    assert kwargs1["json"] == {"context": ctx, "keyring_service": "svc"}

    creds = {
        "name": "c",
        "user": "u",
        "host": "h",
        "port": 1,
        "database": "d",
        "password": "p",
    }
    out2 = client.create_credentials(creds, keyring_service=None)
    assert out2 == {"ok": True}
    _, kwargs2 = session.post.call_args
    assert kwargs2["json"] == {"credentials": creds, "keyring_service": None}

    mapping = {"name": "m", "environment": "DEV", "credentials_ids": {"DEV": "cid"}}
    out3 = client.create_context_mapping(mapping)
    assert out3 == {"ok": True}
    _, kwargs3 = session.post.call_args
    assert kwargs3["json"] == {"context": mapping}


def test_local_execution_start_with_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_handler = Mock()
    runtime_job = Mock(id="j1", model_dump=lambda: {"id": "j1"})
    job_handler.load_runtime_job.return_value = runtime_job
    exec_handler = Mock()
    execution = Mock(id="e1", max_attempts=3)
    exec_handler.execute_job.return_value = execution
    records = Mock()

    monkeypatch.setattr(adapters, "_jh_singleton", lambda: job_handler)
    monkeypatch.setattr(adapters, "_eh_singleton", lambda: exec_handler)
    monkeypatch.setattr(adapters, "_erh_singleton", lambda: records)

    client = adapters.LocalExecutionClient()
    env = SimpleNamespace(value="TEST")
    result = client.start("j1", environment=env)  # type: ignore[arg-type]
    assert result["environment"] == "TEST"
    assert result["execution_id"] == "e1"
