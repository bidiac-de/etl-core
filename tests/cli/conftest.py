from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Generator
from unittest.mock import Mock, patch

import pytest

from tests.helpers import (
    get_sample_job_config,
    create_temp_job_config,
    cleanup_temp_file,
)


@pytest.fixture
def sample_job_config() -> Dict[str, Any]:
    return get_sample_job_config()


@pytest.fixture
def temp_config_file(sample_job_config: Dict[str, Any]) -> Generator[Path, None, None]:
    file_path = create_temp_job_config(sample_job_config)
    yield file_path
    cleanup_temp_file(file_path)


@pytest.fixture
def mock_job_handler():
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
    mock = Mock()
    mock.execute_job.return_value = Mock(id="exec-123", max_attempts=3)
    return mock


@pytest.fixture
def mock_singletons():
    with (
        patch("etl_core.api.cli.adapters._jh_singleton") as jh,
        patch("etl_core.api.cli.adapters._eh_singleton") as eh,
    ):
        jh.return_value = Mock()
        eh.return_value = Mock()
        yield jh, eh
