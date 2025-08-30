from __future__ import annotations

import pytest
from unittest.mock import Mock, patch
from typer.testing import CliRunner
from pathlib import Path
import json

from etl_core.api.cli import app


class TestCLIErrorHandling:
    """Test CLI error handling and edge cases."""

    @pytest.fixture
    def runner(self):
        """CLI test runner."""
        return CliRunner()

    def test_create_job_file_not_found(self, runner):
        """Test create_job with non-existent file."""
        result = runner.invoke(app, ["create-job", "non_existent_file.json"])

        assert result.exit_code != 0
        # Check that an exception was raised
        assert result.exception is not None
        # Check for the error message content
        assert "No such file or directory" in str(result.exception)

    def test_create_job_invalid_json(self, runner):
        """Test create_job with invalid JSON file."""
        # Create a temporary file with invalid JSON
        temp_file = Path("invalid.json")
        temp_file.write_text("invalid json content")

        try:
            result = runner.invoke(app, ["create-job", str(temp_file)])
            assert result.exit_code != 0
            assert result.exception is not None
            # Check for the actual error message content
            assert "Expecting value" in str(result.exception)
        finally:
            # Use the helper function for cleanup to avoid permission issues
            from tests.helpers import cleanup_temp_file

            cleanup_temp_file(temp_file)

    def test_create_job_empty_json(self, runner):
        """Test create_job with empty JSON file."""
        temp_file = Path("empty.json")
        temp_file.write_text("{}")

        try:
            result = runner.invoke(app, ["create-job", str(temp_file)])
            # Should succeed with empty config
            assert result.exit_code == 0
        finally:
            # Use the helper function for cleanup to avoid permission issues
            from tests.helpers import cleanup_temp_file

            cleanup_temp_file(temp_file)

    def test_update_job_file_not_found(self, runner):
        """Test update_job with non-existent file."""
        result = runner.invoke(app, ["update-job", "job-id", "non_existent_file.json"])

        assert result.exit_code != 0
        assert result.exception is not None
        # Check for the actual error message content
        assert "No such file or directory" in str(result.exception)

    def test_update_job_invalid_json(self, runner):
        """Test update_job with invalid JSON file."""
        json_file = Path("tests/components/data/json/testdata_bad.json")

        result = runner.invoke(app, ["update-job", "job-id", str(json_file)])
        assert result.exit_code != 0
        assert result.exception is not None
        # Check for the actual error message content
        assert "Expecting property name" in str(result.exception)

    def test_http_client_connection_error(self, runner):
        """Test HTTP client with connection error."""
        temp_file = Path("test_config.json")
        temp_file.write_text('{"name": "test_job"}')

        try:
            with patch("etl_core.api.cli.requests") as mock_requests:
                mock_requests.post.side_effect = Exception("Connection failed")

                result = runner.invoke(
                    app,
                    [
                        "create-job",
                        str(temp_file),
                        "--remote",
                        "--base-url",
                        "http://invalid-host:9999",
                    ],
                )

                assert result.exit_code != 0
                assert "Connection failed" in str(result.exception)
        finally:
            # Use the helper function for cleanup to avoid permission issues
            from tests.helpers import cleanup_temp_file

            cleanup_temp_file(temp_file)

    def test_http_client_http_error(self, runner):
        """Test HTTP client with HTTP error response."""
        temp_file = Path("test_config.json")
        temp_file.write_text('{"name": "test_job"}')

        try:
            with patch("etl_core.api.cli.requests") as mock_requests:
                mock_response = Mock()
                mock_response.status_code = 500
                mock_response.raise_for_status.side_effect = Exception(
                    "Internal Server Error"
                )
                mock_requests.post.return_value = mock_response

                result = runner.invoke(
                    app,
                    [
                        "create-job",
                        str(temp_file),
                        "--remote",
                        "--base-url",
                        "http://localhost:8000",
                    ],
                )

                assert result.exit_code != 0
                assert "Internal Server Error" in str(result.exception)
        finally:
            # Use the helper function for cleanup to avoid permission issues
            from tests.helpers import cleanup_temp_file

            cleanup_temp_file(temp_file)

    def test_missing_required_arguments(self, runner):
        """Test CLI commands with missing required arguments."""
        # Test create-job without path
        result = runner.invoke(app, ["create-job"])
        assert result.exit_code != 0

        # Test get-job without job_id
        result = runner.invoke(app, ["get-job"])
        assert result.exit_code != 0

        # Test update-job without job_id
        result = runner.invoke(app, ["update-job"])
        assert result.exit_code != 0

        # Test update-job without path
        result = runner.invoke(app, ["update-job", "job-id"])
        assert result.exit_code != 0

        # Test delete-job without job_id
        result = runner.invoke(app, ["delete-job"])
        assert result.exit_code != 0

        # Test start-execution without job_id
        result = runner.invoke(app, ["start-execution"])
        assert result.exit_code != 0

    def test_invalid_base_url_format(self, runner):
        """Test CLI with invalid base URL format."""
        temp_file = Path("test_config.json")
        temp_file.write_text('{"name": "test_job"}')

        try:
            # Test with invalid URL format
            result = runner.invoke(
                app,
                ["create-job", str(temp_file), "--remote", "--base-url", "invalid-url"],
            )

            # Should fail due to invalid URL format
            assert result.exit_code != 0
            assert result.exception is not None
        finally:
            # Use the helper function for cleanup to avoid permission issues
            from tests.helpers import cleanup_temp_file

            cleanup_temp_file(temp_file)

    def test_remote_flag_without_base_url(self, runner):
        """Test remote flag without base URL (should use default)."""
        temp_file = Path("test_config.json")
        temp_file.write_text('{"name": "test_job"}')

        try:
            with patch("etl_core.api.cli.requests") as mock_requests:
                mock_response = Mock()
                mock_response.json.return_value = {"id": "test-job-id-123"}
                mock_response.raise_for_status.return_value = None
                mock_requests.post.return_value = mock_response

                result = runner.invoke(app, ["create-job", str(temp_file), "--remote"])

                assert result.exit_code == 0
                # Should use default base URL
                mock_requests.post.assert_called_once_with(
                    "http://127.0.0.1:8000/jobs", json={"name": "test_job"}
                )
        finally:
            # Use the helper function for cleanup to avoid permission issues
            from tests.helpers import cleanup_temp_file

            cleanup_temp_file(temp_file)

    def test_job_config_validation_errors(self, runner):
        """Test CLI with job config that has validation errors."""
        # Create config with invalid component type
        invalid_config = {
            "name": "test_job",
            "components": [
                {
                    "comp_type": "non_existent_type",
                    "name": "component_a",
                    "description": "Invalid component",
                    "next": [],
                }
            ],
        }

        temp_file = Path("invalid_config.json")
        temp_file.write_text(json.dumps(invalid_config))

        try:
            result = runner.invoke(app, ["create-job", str(temp_file)])
            # Should fail due to validation error
            assert result.exit_code != 0
        finally:
            # Use the helper function for cleanup to avoid permission issues
            from tests.helpers import cleanup_temp_file

            cleanup_temp_file(temp_file)

    def test_unicode_in_job_config(self, runner):
        """Test CLI with Unicode characters in job config."""
        unicode_config = {
            "name": "tëst_job_ümlaut",
            "description": "Test job with Unicode characters",
            "components": [],
        }

        temp_file = Path("unicode_config.json")
        # Use UTF-8 encoding explicitly
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(unicode_config, f, ensure_ascii=False)

        try:
            with patch("etl_core.api.cli._jh_singleton") as mock_jh:
                mock_jh.return_value.create_job_entry.return_value = Mock(
                    id="unicode-job-123"
                )

                result = runner.invoke(app, ["create-job", str(temp_file)])
                assert result.exit_code == 0
                assert "Created job unicode-job-123" in result.stdout
        finally:
            # Use the helper function for cleanup to avoid permission issues
            from tests.helpers import cleanup_temp_file

            cleanup_temp_file(temp_file)
