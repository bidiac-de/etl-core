from __future__ import annotations

import pytest
from unittest.mock import Mock, patch
from pathlib import Path
from typer.testing import CliRunner

from etl_core.api.cli import (
    app,
    LocalJobsClient,
    LocalExecutionClient,
    HttpJobsClient,
    HttpExecutionClient,
    _pick_clients,
)
from etl_core.persistance.errors import PersistNotFoundError


class TestLocalJobsClient:
    """Test local jobs client implementation."""

    def test_create_job_success(self, mock_job_handler):
        """Test successful job creation."""
        with patch("etl_core.api.cli._jh_singleton", return_value=mock_job_handler):
            client = LocalJobsClient()
            config = {"name": "test_job"}

            result = client.create(config)

            assert result == "test-job-id-123"
            mock_job_handler.create_job_entry.assert_called_once()

    def test_get_job_success(self, mock_job_handler):
        """Test successful job retrieval."""
        with patch("etl_core.api.cli._jh_singleton", return_value=mock_job_handler):
            client = LocalJobsClient()

            result = client.get("test-job-id-123")

            assert result["id"] == "test-job-id-123"
            mock_job_handler.load_runtime_job.assert_called_once_with("test-job-id-123")

    def test_update_job_success(self, mock_job_handler):
        """Test successful job update."""
        with patch("etl_core.api.cli._jh_singleton", return_value=mock_job_handler):
            client = LocalJobsClient()
            config = {"name": "updated_job"}

            result = client.update("test-job-id-123", config)

            assert result == "test-job-id-123"
            # Check that update was called with correct arguments
            mock_job_handler.update.assert_called_once()
            call_args = mock_job_handler.update.call_args
            assert call_args[0][0] == "test-job-id-123"  # job_id
            assert call_args[0][1].name == "updated_job"  # JobConfig object

    def test_delete_job_success(self, mock_job_handler):
        """Test successful job deletion."""
        with patch("etl_core.api.cli._jh_singleton", return_value=mock_job_handler):
            client = LocalJobsClient()

            client.delete("test-job-id-123")

            mock_job_handler.delete.assert_called_once_with("test-job-id-123")

    def test_list_jobs_brief_success(self, mock_job_handler):
        """Test successful job listing."""
        with patch("etl_core.api.cli._jh_singleton", return_value=mock_job_handler):
            client = LocalJobsClient()

            result = client.list_brief()

            assert len(result) == 2
            assert result[0]["id"] == "job-1"
            mock_job_handler.list_jobs_brief.assert_called_once()


class TestLocalExecutionClient:
    """Test local execution client implementation."""

    def test_start_execution_success(self, mock_job_handler, mock_execution_handler):
        """Test successful execution start."""
        with (
            patch("etl_core.api.cli._jh_singleton", return_value=mock_job_handler),
            patch(
                "etl_core.api.cli._eh_singleton", return_value=mock_execution_handler
            ),
        ):

            client = LocalExecutionClient()

            result = client.start("test-job-id-123")

            assert result["job_id"] == "test-job-id-123"
            assert result["status"] == "started"
            assert result["execution_id"] == "exec-123"
            assert result["max_attempts"] == 3


class TestHttpJobsClient:
    """Test HTTP jobs client implementation."""

    def test_create_job_success(self, mock_requests):
        """Test successful HTTP job creation."""
        client = HttpJobsClient("http://localhost:8000")
        config = {"name": "test_job"}

        result = client.create(config)

        assert result == {"id": "test-job-id-123"}
        mock_requests.post.assert_called_once_with(
            "http://localhost:8000/jobs", json=config
        )

    def test_get_job_success(self, mock_requests):
        """Test successful HTTP job retrieval."""
        client = HttpJobsClient("http://localhost:8000")

        result = client.get("test-job-id-123")

        assert result == {"id": "test-job-id-123"}
        mock_requests.get.assert_called_once_with(
            "http://localhost:8000/jobs/test-job-id-123"
        )

    def test_get_job_not_found(self, mock_requests):
        """Test HTTP job retrieval with 404 error."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_requests.get.return_value = mock_response

        client = HttpJobsClient("http://localhost:8000")

        with pytest.raises(
            PersistNotFoundError, match="Job 'test-job-id-123' not found"
        ):
            client.get("test-job-id-123")

    def test_update_job_success(self, mock_requests):
        """Test successful HTTP job update."""
        client = HttpJobsClient("http://localhost:8000")
        config = {"name": "updated_job"}

        result = client.update("test-job-id-123", config)

        assert result == {"id": "test-job-id-123"}
        mock_requests.put.assert_called_once_with(
            "http://localhost:8000/jobs/test-job-id-123", json=config
        )

    def test_update_job_not_found(self, mock_requests):
        """Test HTTP job update with 404 error."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_requests.put.return_value = mock_response

        client = HttpJobsClient("http://localhost:8000")

        with pytest.raises(
            PersistNotFoundError, match="Job 'test-job-id-123' not found"
        ):
            client.update("test-job-id-123", {"name": "updated_job"})

    def test_delete_job_success(self, mock_requests):
        """Test successful HTTP job deletion."""
        client = HttpJobsClient("http://localhost:8000")

        client.delete("test-job-id-123")

        mock_requests.delete.assert_called_once_with(
            "http://localhost:8000/jobs/test-job-id-123"
        )

    def test_delete_job_not_found(self, mock_requests):
        """Test HTTP job deletion with 404 error."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_requests.delete.return_value = mock_response

        client = HttpJobsClient("http://localhost:8000")

        with pytest.raises(
            PersistNotFoundError, match="Job 'test-job-id-123' not found"
        ):
            client.delete("test-job-id-123")

    def test_list_jobs_brief_success(self, mock_requests):
        """Test successful HTTP job listing."""
        client = HttpJobsClient("http://localhost:8000")

        result = client.list_brief()

        assert result == {"id": "test-job-id-123"}
        mock_requests.get.assert_called_once_with("http://localhost:8000/jobs")

    def test_base_url_stripping(self):
        """Test that base URL trailing slashes are stripped."""
        client = HttpJobsClient("http://localhost:8000/")
        assert client.base_url == "http://localhost:8000"


class TestHttpExecutionClient:
    """Test HTTP execution client implementation."""

    def test_start_execution_success(self, mock_requests):
        """Test successful HTTP execution start."""
        client = HttpExecutionClient("http://localhost:8000")

        result = client.start("test-job-id-123")

        assert result == {"id": "test-job-id-123"}
        mock_requests.post.assert_called_once_with(
            "http://localhost:8000/execution/test-job-id-123"
        )

    def test_start_execution_not_found(self, mock_requests):
        """Test HTTP execution start with 404 error."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_requests.post.return_value = mock_response

        client = HttpExecutionClient("http://localhost:8000")

        with pytest.raises(
            PersistNotFoundError, match="Job 'test-job-id-123' not found"
        ):
            client.start("test-job-id-123")

    def test_base_url_stripping(self):
        """Test that base URL trailing slashes are stripped."""
        client = HttpExecutionClient("http://localhost:8000/")
        assert client.base_url == "http://localhost:8000"


class TestClientSelection:
    """Test client selection logic."""

    def test_pick_local_clients(self):
        """Test local client selection."""
        jobs_client, exec_client = _pick_clients(remote=False, base_url="")

        assert isinstance(jobs_client, LocalJobsClient)
        assert isinstance(exec_client, LocalExecutionClient)

    def test_pick_http_clients(self):
        """Test HTTP client selection."""
        jobs_client, exec_client = _pick_clients(
            remote=True, base_url="http://localhost:8000"
        )

        assert isinstance(jobs_client, HttpJobsClient)
        assert isinstance(exec_client, HttpExecutionClient)
        assert jobs_client.base_url == "http://localhost:8000"
        assert exec_client.base_url == "http://localhost:8000"


class TestCLICommands:
    """Test CLI command implementations."""

    @pytest.fixture
    def runner(self):
        """CLI test runner."""
        return CliRunner()

    def test_create_job_command_local(self, runner, temp_config_file, mock_singletons):
        """Test create_job command with local execution."""
        mock_jh, mock_eh = mock_singletons
        mock_jh.return_value.create_job_entry.return_value = Mock(id="new-job-123")

        result = runner.invoke(app, ["create-job", str(temp_config_file)])

        assert result.exit_code == 0
        assert "Created job new-job-123" in result.stdout

    def test_create_job_command_remote(self, runner, temp_config_file, mock_requests):
        """Test create_job command with remote execution."""
        result = runner.invoke(
            app,
            [
                "create-job",
                str(temp_config_file),
                "--remote",
                "--base-url",
                "http://localhost:8000",
            ],
        )

        assert result.exit_code == 0
        # The response is a dict, so check for the ID value
        assert "test-job-id-123" in result.stdout

    def test_get_job_command_local(self, runner, mock_singletons):
        """Test get_job command with local execution."""
        mock_jh, mock_eh = mock_singletons
        mock_job = Mock()
        mock_job.model_dump.return_value = {"name": "test_job"}
        mock_job.id = "test-job-id-123"
        mock_jh.return_value.load_runtime_job.return_value = mock_job

        result = runner.invoke(app, ["get-job", "test-job-id-123"])

        assert result.exit_code == 0
        assert "test-job-id-123" in result.stdout

    def test_get_job_command_not_found(self, runner, mock_singletons):
        """Test get_job command with non-existent job."""
        mock_jh, mock_eh = mock_singletons
        mock_jh.return_value.load_runtime_job.side_effect = PersistNotFoundError(
            "Job not found"
        )

        result = runner.invoke(app, ["get-job", "non-existent-id"])

        assert result.exit_code == 1
        assert "not found" in result.stdout

    def test_update_job_command_local(self, runner, temp_config_file, mock_singletons):
        """Test update_job command with local execution."""
        mock_jh, mock_eh = mock_singletons
        mock_jh.return_value.update.return_value = Mock(id="updated-job-123")

        result = runner.invoke(
            app, ["update-job", "test-job-id-123", str(temp_config_file)]
        )

        assert result.exit_code == 0
        assert "Updated job updated-job-123" in result.stdout

    def test_update_job_command_not_found(
        self, runner, temp_config_file, mock_singletons
    ):
        """Test update_job command with non-existent job."""
        mock_jh, mock_eh = mock_singletons
        mock_jh.return_value.update.side_effect = PersistNotFoundError("Job not found")

        result = runner.invoke(
            app, ["update-job", "non-existent-id", str(temp_config_file)]
        )

        assert result.exit_code == 1
        assert "not found" in result.stdout

    def test_delete_job_command_local(self, runner, mock_singletons):
        """Test delete_job command with local execution."""
        mock_jh, mock_eh = mock_singletons

        result = runner.invoke(app, ["delete-job", "test-job-id-123"])

        assert result.exit_code == 0
        assert "Deleted job test-job-id-123" in result.stdout

    def test_delete_job_command_not_found(self, runner, mock_singletons):
        """Test delete_job command with non-existent job."""
        mock_jh, mock_eh = mock_singletons
        mock_jh.return_value.delete.side_effect = PersistNotFoundError("Job not found")

        result = runner.invoke(app, ["delete-job", "non-existent-id"])

        assert result.exit_code == 1
        assert "not found" in result.stdout

    def test_list_jobs_command_local(self, runner, mock_singletons):
        """Test list_jobs command with local execution."""
        mock_jh, mock_eh = mock_singletons
        mock_jh.return_value.list_jobs_brief.return_value = [
            {"id": "job-1", "name": "Test Job 1"}
        ]

        result = runner.invoke(app, ["list-jobs"])

        assert result.exit_code == 0
        assert "job-1" in result.stdout

    def test_start_execution_command_local(self, runner, mock_singletons):
        """Test start_execution command with local execution."""
        mock_jh, mock_eh = mock_singletons
        mock_job = Mock()
        mock_job.id = "test-job-id-123"
        mock_jh.return_value.load_runtime_job.return_value = mock_job

        mock_execution = Mock()
        mock_execution.id = "exec-123"
        mock_execution.max_attempts = 3
        mock_eh.return_value.execute_job.return_value = mock_execution

        result = runner.invoke(app, ["start-execution", "test-job-id-123"])

        assert result.exit_code == 0
        assert "test-job-id-123" in result.stdout
        assert "started" in result.stdout

    def test_start_execution_command_not_found(self, runner, mock_singletons):
        """Test start_execution command with non-existent job."""
        mock_jh, mock_eh = mock_singletons
        mock_jh.return_value.load_runtime_job.side_effect = PersistNotFoundError(
            "Job not found"
        )

        result = runner.invoke(app, ["start-execution", "non-existent-id"])

        assert result.exit_code == 1
        assert "not found" in result.stdout

    def test_invalid_json_file(self, runner):
        """Test CLI with invalid JSON file."""
        # Use existing bad JSON file
        json_file = Path("tests/components/data/json/testdata_bad.json")

        result = runner.invoke(app, ["create-job", str(json_file)])
        assert result.exit_code != 0
        assert result.exception is not None
        # Check for the actual error message content
        assert "Expecting property name" in str(result.exception)


    def test_help_output(self, runner):
        """Test CLI help output."""
        result = runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "ETL control CLI" in result.stdout
        assert "create-job" in result.stdout
        assert "get-job" in result.stdout
        assert "update-job" in result.stdout
        assert "delete-job" in result.stdout
        assert "list-jobs" in result.stdout
        assert "start-execution" in result.stdout
