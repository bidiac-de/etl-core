from __future__ import annotations

import pytest
from unittest.mock import Mock, patch
from typing import Generator, Dict, Any
from pathlib import Path

from tests.helpers import (
    get_sample_job_config,
    create_temp_job_config,
    cleanup_temp_file,
)


@pytest.fixture
def sample_job_config() -> Dict[str, Any]:
    """Provide a sample job configuration for testing."""
    return get_sample_job_config()


@pytest.fixture
def temp_config_file(sample_job_config: Dict[str, Any]) -> Generator[Path, None, None]:
    """Create a temporary config file and clean it up after the test."""
    file_path = create_temp_job_config(sample_job_config)
    yield file_path
    cleanup_temp_file(file_path)


@pytest.fixture
def mock_job_handler():
    """Mock job handler for CLI testing."""
    mock = Mock()
    mock.create_job_entry.return_value = Mock(id="test-job-id-123")
    mock.load_runtime_job.return_value = Mock(
        id="test-job-id-123", model_dump=lambda: get_sample_job_config()
    )
    mock.update.return_value = Mock(id="test-job-id-123")
    mock.list_jobs_brief.return_value = [
        {"id": "job-1", "name": "Test Job 1"},
        {"id": "job-2", "name": "Test Job 2"},
    ]
    return mock


@pytest.fixture
def mock_execution_handler():
    """Mock execution handler for CLI testing."""
    mock = Mock()
    mock.execute_job.return_value = Mock(id="exec-123", max_attempts=3)
    return mock


@pytest.fixture
def mock_requests():
    """Mock requests module for HTTP client testing."""
    with patch("etl_core.api.cli.requests") as mock_req:
        # Mock successful responses
        mock_response = Mock()
        mock_response.json.return_value = {"id": "test-job-id-123"}
        mock_response.raise_for_status.return_value = None
        mock_req.post.return_value = mock_response
        mock_req.get.return_value = mock_response
        mock_req.put.return_value = mock_response
        mock_req.delete.return_value = mock_response

        yield mock_req


@pytest.fixture
def mock_singletons():
    """Mock singleton modules for CLI testing."""
    with (
        patch("etl_core.api.cli._jh_singleton") as mock_jh,
        patch("etl_core.api.cli._eh_singleton") as mock_eh,
    ):

        mock_jh.return_value = Mock()
        mock_eh.return_value = Mock()

        yield mock_jh, mock_eh
