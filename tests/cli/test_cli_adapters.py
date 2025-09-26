import pytest
from unittest.mock import Mock

import etl_core.api.cli.adapters as adapters
from etl_core.api.cli.adapters import (
    LocalJobsClient,
    LocalExecutionClient,
    LocalContextsClient,
    _dedupe,
    api_base_url,
)


def test_local_jobs_client_crud(monkeypatch, mock_job_handler):
    monkeypatch.setattr(adapters, "_jh_singleton", lambda: mock_job_handler)
    client = LocalJobsClient()

    # create
    job_id = client.create({"name": "test"})
    assert job_id == "test-job-id-123"

    # get
    job = client.get("test-job-id-123")
    assert job["id"] == "test-job-id-123"
    assert "name" in job

    # update
    job_id2 = client.update("test-job-id-123", {"name": "updated"})
    assert job_id2 == "test-job-id-123"

    # delete (no return)
    client.delete("test-job-id-123")
    mock_job_handler.delete.assert_called_once()

    # list_brief
    jobs = client.list_brief()
    assert len(jobs) == 2

def test_local_execution_client_start_and_get(monkeypatch, mock_job_handler, mock_execution_handler):
    monkeypatch.setattr(adapters, "_jh_singleton", lambda: mock_job_handler)
    monkeypatch.setattr(adapters, "_eh_singleton", lambda: mock_execution_handler)
    monkeypatch.setattr(adapters, "_erh_singleton", lambda: Mock())

    client = LocalExecutionClient()

    # start
    result = client.start("test-job-id-123")
    assert result["status"] == "started"
    assert "execution_id" in result

    # get with missing row raises
    records = client._records
    records.get_execution.return_value = (None, [])
    with pytest.raises(adapters.PersistNotFoundError):
        client.get("missing-id")

    # get with row
    row = Mock(id="exec-1", job_id="j1", environment="dev",
               status="ok", error=None,
               started_at=Mock(isoformat=lambda: "2021-01-01T00:00:00"),
               finished_at=None)
    attempt = Mock(id="a1", execution_id="exec-1", attempt_index=1,
                   status="ok", error=None,
                   started_at=Mock(isoformat=lambda: "2021-01-01T00:00:00"),
                   finished_at=None)
    records.get_execution.return_value = (row, [attempt])
    out = client.get("exec-1")
    assert out["execution"]["id"] == "exec-1"
    assert out["attempts"][0]["id"] == "a1"


def test_local_execution_list_and_attempts(monkeypatch):
    records = Mock()
    row = Mock(id="exec-2", job_id="j2", environment="prod",
               status="done", error=None,
               started_at=Mock(isoformat=lambda: "t0"),
               finished_at=Mock(isoformat=lambda: "t1"))
    records.list_executions.return_value = ([row], 1)
    records.get_execution.return_value = (row, [])
    records.list_attempts.return_value = [row]

    monkeypatch.setattr(adapters, "_erh_singleton", lambda: records)
    monkeypatch.setattr(adapters, "_jh_singleton", lambda: Mock())
    monkeypatch.setattr(adapters, "_eh_singleton", lambda: Mock())

    client = LocalExecutionClient()
    data = client.list_executions()
    assert data["data"][0]["id"] == "exec-2"

    # attempts raises if no row
    records.get_execution.return_value = (None, [])
    with pytest.raises(adapters.PersistNotFoundError):
        client.attempts("x")

    # valid attempts
    records.get_execution.return_value = (row, [])
    out = client.attempts("exec-2")
    assert out[0]["id"] == "exec-2"


def test_local_execution_parse_dt():
    dt = LocalExecutionClient._parse_dt("2020-01-01T12:00:00")
    assert dt.year == 2020


def test_local_contexts_secret_store_failure(monkeypatch):
    client = LocalContextsClient()
    monkeypatch.setattr(adapters, "create_secret_provider", lambda: (_ for _ in ()).throw(Exception("boom")))
    with pytest.raises(RuntimeError):
        client._secret_store()


def test_local_contexts_create_and_get(monkeypatch):
    ctx_handler = Mock()
    creds_handler = Mock()
    monkeypatch.setattr(adapters, "_ch_singleton", lambda: ctx_handler)
    monkeypatch.setattr(adapters, "_crh_singleton", lambda: creds_handler)
    monkeypatch.setattr(adapters, "create_secret_provider", lambda: Mock())
    monkeypatch.setattr(
        adapters, "SecureContextAdapter",
        lambda **kw: Mock(bootstrap_to_store=lambda: None),
    )

    client = LocalContextsClient()

    # create context (ENV MUST BE UPPERCASE: DEV|TEST|PROD)
    out = client.create_context(
        {"name": "ctx", "environment": "DEV", "parameters": {}},
        None,
    )
    assert out["kind"] == "context"

    # create credentials
    out2 = client.create_credentials(
        {
            "name": "c",
            "user": "u",
            "host": "h",
            "port": 1,
            "database": "db",
            "password": "pw",
        },
        None,
    )
    assert out2["kind"] == "credentials"

    # context mapping with unknown creds
    creds_handler.get_by_id.return_value = None
    with pytest.raises(adapters.PersistNotFoundError):
        client.create_context_mapping(
            {"name": "m", "environment": "DEV", "credentials_ids": {"DEV": "x"}}
        )

    ctx_handler.list_all.return_value = [Mock(id="c1")]
    creds_handler.list_all.return_value = [Mock(id="c2")]
    providers = client.list_providers()
    assert {"id": "c1", "kind": "context"} in providers
    assert {"id": "c2", "kind": "credentials"} in providers

    ctx_handler.get_by_id.return_value = Mock(environment="DEV")
    assert client.get_provider("c1")["kind"] == "context"

    ctx_handler.get_by_id.return_value = None
    creds_handler.get_by_id.return_value = Mock()
    assert client.get_provider("c2")["kind"] == "credentials"

    creds_handler.get_by_id.return_value = None
    with pytest.raises(adapters.PersistNotFoundError):
        client.get_provider("missing")

    ctx_handler.delete_by_id.side_effect = adapters.PersistNotFoundError("x")
    creds_handler.delete_by_id.side_effect = adapters.PersistNotFoundError("x")
    client.delete_provider("any")
    ctx_handler.delete_by_id.assert_called()
    creds_handler.delete_by_id.assert_called()

def test_sanitize_url_edge_cases():
    assert adapters._RestBase._sanitize_url(None) == "<unknown>"

    assert adapters._RestBase._sanitize_url("http://[::1") == "<invalid-url>"

    assert adapters._RestBase._sanitize_url("not a url://") == "not a url://"

    s = adapters._RestBase._sanitize_url("https://user:pass@h.com/p?q=1#frag")
    assert s == "https://***@h.com/p"

@pytest.mark.parametrize("cls,method,url", [
    (adapters.RemoteJobsClient, "get", "/jobs/123"),
    (adapters.RemoteExecutionClient, "get", "/execution/executions/123"),
    (adapters.RemoteContextsClient, "list_providers", "/contexts/"),
])
def test_remote_clients_methods(monkeypatch, cls, method, url):
    resp = Mock()
    resp.json.return_value = {"ok": True}
    monkeypatch.setattr(adapters, "requests", Mock(Session=lambda: Mock()))
    client = cls("http://base")
    client.session = Mock()
    client.session.get.return_value = resp
    client._raise_for_status = lambda r: None

    fn = getattr(client, method)
    result = fn("123") if "123" in url else fn()
    assert result == {"ok": True}
