from __future__ import annotations

from unittest.mock import Mock, patch

import pytest
from typer.testing import CliRunner

from etl_core.api.cli.main import app
from etl_core.api.cli.adapters import (
    LocalJobsClient,
    LocalExecutionClient,
    HttpJobsClient,
    HttpExecutionClient,
)
from etl_core.api.cli.wiring import pick_clients as _pick_clients
from etl_core.persistance.errors import PersistNotFoundError


class TestLocalJobsClient:
    def test_create_job_success(self, mock_job_handler):
        with patch(
            "etl_core.api.cli.adapters._jh_singleton", return_value=mock_job_handler
        ):
            client = LocalJobsClient()
            result = client.create({"name": "test_job"})
            assert result == "test-job-id-123"

    def test_get_job_success(self, mock_job_handler):
        with patch(
            "etl_core.api.cli.adapters._jh_singleton", return_value=mock_job_handler
        ):
            client = LocalJobsClient()
            result = client.get("test-job-id-123")
            assert result["id"] == "test-job-id-123"

    def test_update_job_success(self, mock_job_handler):
        with patch(
            "etl_core.api.cli.adapters._jh_singleton", return_value=mock_job_handler
        ):
            client = LocalJobsClient()
            result = client.update("test-job-id-123", {"name": "updated"})
            assert result == "test-job-id-123"

    def test_delete_job_success(self, mock_job_handler):
        with patch(
            "etl_core.api.cli.adapters._jh_singleton", return_value=mock_job_handler
        ):
            client = LocalJobsClient()
            client.delete("test-job-id-123")
            mock_job_handler.delete.assert_called_once_with("test-job-id-123")

    def test_list_jobs_brief_success(self, mock_job_handler):
        with patch(
            "etl_core.api.cli.adapters._jh_singleton", return_value=mock_job_handler
        ):
            client = LocalJobsClient()
            rows = client.list_brief()
            assert len(rows) == 2
            assert rows[0]["id"] == "job-1"


class TestLocalExecutionClient:
    def test_start_execution_success(self, mock_job_handler, mock_execution_handler):
        with (
            patch(
                "etl_core.api.cli.adapters._jh_singleton",
                return_value=mock_job_handler,
            ),
            patch(
                "etl_core.api.cli.adapters._eh_singleton",
                return_value=mock_execution_handler,
            ),
        ):
            client = LocalExecutionClient()
            result = client.start("test-job-id-123")
            assert result["job_id"] == "test-job-id-123"
            assert result["status"] == "started"
            assert result["execution_id"] == "exec-123"
            assert result["max_attempts"] == 3


class TestHttpJobsClient:
    def test_create_job_success(self, mock_requests):
        client = HttpJobsClient("http://localhost:8000")
        cfg = {"name": "test_job"}
        result = client.create(cfg)
        assert result == {"id": "test-job-id-123"}
        mock_requests.post.assert_called_once_with(
            "http://localhost:8000/jobs", json=cfg, timeout=30
        )

    def test_get_job_success(self, mock_requests):
        client = HttpJobsClient("http://localhost:8000")
        result = client.get("test-job-id-123")
        assert result == {"id": "test-job-id-123"}
        mock_requests.get.assert_called_once_with(
            "http://localhost:8000/jobs/test-job-id-123", timeout=30
        )

    def test_get_job_not_found(self, mock_requests):
        mock = Mock()
        mock.status_code = 404
        mock_requests.get.return_value = mock
        client = HttpJobsClient("http://localhost:8000")
        with pytest.raises(
            PersistNotFoundError, match="Job 'test-job-id-123' not found"
        ):
            client.get("test-job-id-123")

    def test_update_job_success(self, mock_requests):
        client = HttpJobsClient("http://localhost:8000")
        result = client.update("test-job-id-123", {"name": "updated"})
        assert result == {"id": "test-job-id-123"}
        mock_requests.put.assert_called_once_with(
            "http://localhost:8000/jobs/test-job-id-123",
            json={"name": "updated"},
            timeout=30,
        )

    def test_update_job_not_found(self, mock_requests):
        mock = Mock()
        mock.status_code = 404
        mock_requests.put.return_value = mock
        client = HttpJobsClient("http://localhost:8000")
        with pytest.raises(
            PersistNotFoundError, match="Job 'test-job-id-123' not found"
        ):
            client.update("test-job-id-123", {"name": "x"})

    def test_delete_job_success(self, mock_requests):
        client = HttpJobsClient("http://localhost:8000")
        client.delete("test-job-id-123")
        mock_requests.delete.assert_called_once_with(
            "http://localhost:8000/jobs/test-job-id-123", timeout=30
        )

    def test_delete_job_not_found(self, mock_requests):
        mock = Mock()
        mock.status_code = 404
        mock_requests.delete.return_value = mock
        client = HttpJobsClient("http://localhost:8000")
        with pytest.raises(
            PersistNotFoundError, match="Job 'test-job-id-123' not found"
        ):
            client.delete("test-job-id-123")

    def test_list_jobs_brief_success(self, mock_requests):
        client = HttpJobsClient("http://localhost:8000")
        result = client.list_brief()
        assert result == {"id": "test-job-id-123"}
        mock_requests.get.assert_called_once_with(
            "http://localhost:8000/jobs", timeout=30
        )

    def test_base_url_stripping(self):
        c = HttpJobsClient("http://localhost:8000/")
        assert c.base_url == "http://localhost:8000"


class TestHttpExecutionClient:
    def test_start_execution_success(self, mock_requests):
        client = HttpExecutionClient("http://localhost:8000")
        result = client.start("test-job-id-123")
        assert result == {"id": "test-job-id-123"}
        mock_requests.post.assert_called_once_with(
            "http://localhost:8000/execution/test-job-id-123", json=None, timeout=30
        )

    def test_start_execution_not_found(self, mock_requests):
        mock = Mock()
        mock.status_code = 404
        mock_requests.post.return_value = mock
        client = HttpExecutionClient("http://localhost:8000")
        with pytest.raises(
            PersistNotFoundError, match="Job 'test-job-id-123' not found"
        ):
            client.start("test-job-id-123")

    def test_base_url_stripping(self):
        c = HttpExecutionClient("http://localhost:8000/")
        assert c.base_url == "http://localhost:8000"


class TestClientSelection:
    def test_pick_local_clients(self):
        jobs_client, exec_client, _ = _pick_clients(remote=False, base_url="")
        assert isinstance(jobs_client, LocalJobsClient)
        assert isinstance(exec_client, LocalExecutionClient)

    def test_pick_http_clients(self):
        jobs_client, exec_client, _ = _pick_clients(
            remote=True, base_url="http://localhost:8000"
        )
        assert isinstance(jobs_client, HttpJobsClient)
        assert isinstance(exec_client, HttpExecutionClient)
        assert jobs_client.base_url == "http://localhost:8000"
        assert exec_client.base_url == "http://localhost:8000"


class TestCLICommands:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_jobs_create_local(self, runner, temp_config_file, mock_singletons):
        jh, _ = mock_singletons
        jh.return_value.create_job_entry.return_value = Mock(id="new-job-123")
        res = runner.invoke(app, ["jobs", "create", str(temp_config_file)])
        assert res.exit_code == 0
        assert "Created job new-job-123" in res.stdout

    def test_jobs_create_remote(self, runner, temp_config_file, mock_requests):
        res = runner.invoke(
            app,
            [
                "jobs",
                "create",
                str(temp_config_file),
                "--remote",
                "--base-url",
                "http://localhost:8000",
            ],
        )
        assert res.exit_code == 0
        assert "test-job-id-123" in res.stdout

    def test_jobs_get_local(self, runner, mock_singletons):
        jh, _ = mock_singletons
        mock_job = Mock()
        mock_job.model_dump.return_value = {"name": "test_job"}
        mock_job.id = "test-job-id-123"
        jh.return_value.load_runtime_job.return_value = mock_job
        res = runner.invoke(app, ["jobs", "get", "test-job-id-123"])
        assert res.exit_code == 0
        assert "test-job-id-123" in res.stdout

    def test_execution_start_local(self, runner, mock_singletons):
        jh, eh = mock_singletons
        mock_job = Mock()
        mock_job.id = "test-job-id-123"
        jh.return_value.load_runtime_job.return_value = mock_job
        mock_exec = Mock(id="exec-123", max_attempts=3)
        eh.return_value.execute_job.return_value = mock_exec
        res = runner.invoke(app, ["execution", "start", "test-job-id-123"])
        assert res.exit_code == 0
        assert "started" in res.stdout

    def test_help(self, runner):
        res = runner.invoke(app, ["--help"])
        assert res.exit_code == 0
        assert "jobs" in res.stdout
        assert "execution" in res.stdout
