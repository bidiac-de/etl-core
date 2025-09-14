from __future__ import annotations

from unittest.mock import Mock, patch

from typer.testing import CliRunner

from etl_core.api.cli.cli_app import app
from etl_core.api.cli.adapters import LocalJobsClient, LocalExecutionClient
from etl_core.api.cli.wiring import pick_clients as _pick_clients


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


class TestClientSelection:
    def test_pick_local_clients(self):
        jobs_client, exec_client, _ = _pick_clients()
        assert isinstance(jobs_client, LocalJobsClient)
        assert isinstance(exec_client, LocalExecutionClient)


class TestCLICommands:
    @staticmethod
    def runner():
        return CliRunner()

    def test_jobs_create_local(self, temp_config_file, mock_singletons):
        runner = self.runner()
        jh, _ = mock_singletons
        jh.return_value.create_job_entry.return_value = Mock(id="new-job-123")
        res = runner.invoke(app, ["jobs", "create", str(temp_config_file)])
        assert res.exit_code == 0
        assert "Created job new-job-123" in res.stdout

    def test_jobs_get_local(self, mock_singletons):
        runner = self.runner()
        jh, _ = mock_singletons
        mock_job = Mock()
        mock_job.model_dump.return_value = {"name": "test_job"}
        mock_job.id = "test-job-id-123"
        jh.return_value.load_runtime_job.return_value = mock_job
        res = runner.invoke(app, ["jobs", "get", "test-job-id-123"])
        assert res.exit_code == 0
        assert "test-job-id-123" in res.stdout

    def test_execution_start_local(self, mock_singletons):
        runner = self.runner()
        jh, eh = mock_singletons
        mock_job = Mock()
        mock_job.id = "test-job-id-123"
        jh.return_value.load_runtime_job.return_value = mock_job
        mock_exec = Mock(id="exec-123", max_attempts=3)
        eh.return_value.execute_job.return_value = mock_exec
        res = runner.invoke(app, ["execution", "start", "test-job-id-123"])
        assert res.exit_code == 0
        assert "started" in res.stdout

    def test_help(self):
        runner = self.runner()
        res = runner.invoke(app, ["--help"])
        assert res.exit_code == 0
        assert "jobs" in res.stdout
        assert "execution" in res.stdout
