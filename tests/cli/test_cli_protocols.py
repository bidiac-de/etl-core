from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from etl_core.api.cli.ports import JobsPort, ExecutionPort
from etl_core.api.cli.adapters import (
    LocalJobsClient,
    LocalExecutionClient,
    HttpJobsClient,
    HttpExecutionClient,
)
from etl_core.persistance.errors import PersistNotFoundError


class TestProtocolCompliance:
    def test_local_jobs_client_protocol(self) -> None:
        client = LocalJobsClient()
        for name in ("create", "get", "update", "delete", "list_brief"):
            assert hasattr(client, name) and callable(getattr(client, name))

    def test_local_execution_client_protocol(self) -> None:
        client = LocalExecutionClient()
        assert hasattr(client, "start") and callable(client.start)

    def test_http_jobs_client_protocol(self) -> None:
        client = HttpJobsClient("http://localhost:8000")
        for name in ("create", "get", "update", "delete", "list_brief"):
            assert hasattr(client, name) and callable(getattr(client, name))

    def test_http_execution_client_protocol(self) -> None:
        client = HttpExecutionClient("http://localhost:8000")
        assert hasattr(client, "start") and callable(client.start)


class TestProtocolTypeHints:
    def test_jobs_port_annotations(self) -> None:
        assert JobsPort.__annotations__ is not None
        for name in ("create", "get", "update", "delete", "list_brief"):
            assert hasattr(JobsPort, name)

    def test_execution_port_annotations(self) -> None:
        assert ExecutionPort.__annotations__ is not None
        assert hasattr(ExecutionPort, "start")


class TestClientIntegration:
    def test_local_clients_work_together(
        self, mock_job_handler, mock_execution_handler
    ):
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
            jobs_client = LocalJobsClient()
            exec_client = LocalExecutionClient()

            job_id = jobs_client.create({"name": "test_job"})
            assert job_id == "test-job-id-123"

            result = exec_client.start(job_id)
            assert result["job_id"] == job_id
            assert result["status"] == "started"

    def test_http_clients_work_together(self, mock_requests):
        jobs_client = HttpJobsClient("http://localhost:8000")
        exec_client = HttpExecutionClient("http://localhost:8000")

        job_id = jobs_client.create({"name": "test_job"})
        assert job_id == {"id": "test-job-id-123"}

        result = exec_client.start("test-job-id-123")
        assert result == {"id": "test-job-id-123"}

    def test_mixed_isolation(self):
        local_jobs = LocalJobsClient()
        http_jobs = HttpJobsClient("http://localhost:8000")
        assert local_jobs is not http_jobs
        assert type(local_jobs) is not type(http_jobs)


class TestClientStateIndependence:
    def test_local_jobs_state(self):
        c1, c2 = LocalJobsClient(), LocalJobsClient()
        assert c1 is not c2
        c1.job_handler = Mock()
        assert c1.job_handler is not c2.job_handler

    def test_http_jobs_state(self):
        c1 = HttpJobsClient("http://localhost:8000")
        c2 = HttpJobsClient("http://localhost:8001")
        assert c1 is not c2
        assert c1.base_url != c2.base_url

    def test_local_exec_state(self):
        c1, c2 = LocalExecutionClient(), LocalExecutionClient()
        assert c1 is not c2
        c1.exec_handler = Mock()
        assert c1.exec_handler is not c2.exec_handler


class TestErrorPropagation:
    def test_local_exec_raises(self, mock_job_handler):
        with (
            patch(
                "etl_core.api.cli.adapters._jh_singleton",
                return_value=mock_job_handler,
            ),
            patch("etl_core.api.cli.adapters._eh_singleton", return_value=Mock()),
        ):
            mock_job_handler.load_runtime_job.side_effect = PersistNotFoundError(
                "Job not found"
            )
            client = LocalExecutionClient()
            with pytest.raises(PersistNotFoundError, match="Job not found"):
                client.start("missing")

    def test_http_propagates_requests_error(self, mock_requests):
        mock_requests.post.side_effect = Exception("Network error")
        client = HttpJobsClient("http://localhost:8000")
        with pytest.raises(Exception, match="Network error"):
            client.create({"name": "x"})


class TestHttpConfig:
    def test_base_url(self):
        base = "https://api.example.com:8443"
        jobs = HttpJobsClient(base)
        execs = HttpExecutionClient(base)
        assert jobs.base_url == base
        assert execs.base_url == base

    def test_strip_slash(self):
        base = "http://localhost:8000/"
        jobs = HttpJobsClient(base)
        execs = HttpExecutionClient(base)
        assert jobs.base_url == "http://localhost:8000"
        assert execs.base_url == "http://localhost:8000"
