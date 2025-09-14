from __future__ import annotations

from typing import Iterator, Tuple

import pytest
from fastapi.testclient import TestClient
import etl_core.persistence.table_definitions  # noqa: F401

from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from etl_core.persistence.configs.job_config import JobConfig
from etl_core.persistence.handlers.execution_records_handler import (
    ExecutionRecordsHandler,
)
from etl_core.singletons import execution_records_handler
from etl_core.api.routers import execution as execution_router
from etl_core.api.dependencies import get_execution_handler


def _cfg_success(schema_row_min: dict | None = None) -> JobConfig:
    return JobConfig(
        name="ok",
        num_of_retries=0,
        strategy_type="row",
        components=[
            {
                "comp_type": "test",
                "name": "c1",
                "description": "",
                "routes": {"out": []},
                "out_port_schemas": {"out": schema_row_min or {}},
            }
        ],
    )


def _cfg_fail(schema_row_min: dict | None = None) -> JobConfig:
    return JobConfig(
        name="fail",
        num_of_retries=0,
        strategy_type="row",
        components=[
            {
                "comp_type": "failtest",
                "name": "boom",
                "description": "",
                "routes": {"out": []},
                "out_port_schemas": {"out": schema_row_min or {}},
            }
        ],
    )


def _cfg_retry_once(schema_row_min: dict | None = None) -> JobConfig:
    return JobConfig(
        name="retry-once",
        num_of_retries=1,
        strategy_type="row",
        components=[
            {
                "comp_type": "stub_fail_once",
                "name": "flaky",
                "description": "",
                "routes": {"out": []},
                "out_port_schemas": {"out": schema_row_min or {}},
            }
        ],
    )


@pytest.fixture(scope="function")
def patched_records_and_exec_handler() -> (
    Iterator[Tuple[ExecutionRecordsHandler, JobExecutionHandler]]
):
    """
    Shared in-memory engine (StaticPool) so router queries and execution writes
    see the same tables/data.
    """

    records = execution_records_handler()
    exec_handler = JobExecutionHandler()
    exec_handler._exec_records_handler = records  # type: ignore[attr-defined]

    # Patch router's module-level records handler to our in-memory one
    orig_records = getattr(execution_router, "_records", None)
    execution_router._records = records
    try:
        yield records, exec_handler
    finally:
        execution_router._records = orig_records


@pytest.fixture
def dep_override_client(
    client: TestClient, patched_records_and_exec_handler
) -> Iterator[TestClient]:
    """
    Apply dependency override on the real FastAPI app embedded in the provided client.
    """
    _, exec_handler = patched_records_and_exec_handler
    app = client.app
    app.dependency_overrides[get_execution_handler] = lambda: exec_handler
    try:
        yield client
    finally:
        app.dependency_overrides.pop(get_execution_handler, None)


def test_endpoints_persist_success_and_list_filters(
    dep_override_client: TestClient,
    shared_job_handler,  # from your test suite
    schema_row_min,
) -> None:
    client = dep_override_client

    # Create ok job
    ok_id = shared_job_handler.create_job_entry(_cfg_success(schema_row_min)).id

    # Start execution via endpoint
    r = client.post(f"/execution/{ok_id}")
    assert r.status_code == 200
    started = r.json()
    assert started["status"] == "started" and started["job_id"] == ok_id
    exec_id = started["execution_id"]

    # List executions, filter by SUCCESS
    r = client.get("/execution/executions", params={"status": "SUCCESS"})
    assert r.status_code == 200
    payload = r.json()
    assert "data" in payload
    assert any(row["id"] == exec_id for row in payload["data"])

    # Detail endpoint
    r = client.get(f"/execution/executions/{exec_id}")
    assert r.status_code == 200
    detail = r.json()
    assert detail["execution"]["id"] == exec_id
    # should have exactly one successful attempt
    attempts = detail["attempts"]
    assert len(attempts) == 1
    assert attempts[0]["attempt_index"] == 1
    assert attempts[0]["status"] == "SUCCESS"

    # Attempts endpoint returns list, same ordering
    r = client.get(f"/execution/executions/{exec_id}/attempts")
    assert r.status_code == 200
    rows = r.json()
    assert rows and rows[0]["status"] == "SUCCESS" and rows[0]["attempt_index"] == 1


def test_endpoints_persist_failure_and_404s(
    dep_override_client: TestClient,
    shared_job_handler,
    schema_row_min,
) -> None:
    client = dep_override_client

    # Create failing job
    job_id = shared_job_handler.create_job_entry(_cfg_fail(schema_row_min)).id

    # will run and persist FAILED attempt + execution
    r = client.post(f"/execution/{job_id}")
    assert r.status_code == 200
    exec_id = r.json()["execution_id"]

    # Verify FAILED appears in list
    r = client.get("/execution/executions", params={"status": "FAILED"})
    assert r.status_code == 200
    data = r.json()["data"]
    assert any(row["id"] == exec_id and row["status"] == "FAILED" for row in data)

    # Detail should show a single FAILED attempt with error set
    r = client.get(f"/execution/executions/{exec_id}")
    assert r.status_code == 200
    body = r.json()
    assert body["execution"]["status"] == "FAILED"
    assert body["execution"]["error"]  # non-empty
    assert len(body["attempts"]) == 1 and body["attempts"][0]["status"] == "FAILED"
    assert body["attempts"][0]["error"]

    # Missing execution 404s
    r = client.get("/execution/executions/does-not-exist")
    assert r.status_code == 404

    r = client.get("/execution/executions/does-not-exist/attempts")
    assert r.status_code == 404


def test_endpoints_persist_retry_then_success_and_time_filters(
    dep_override_client: TestClient,
    shared_job_handler,
    schema_row_min,
) -> None:
    client = dep_override_client

    # Create a job that fails once then succeeds on retry
    job_id = shared_job_handler.create_job_entry(_cfg_retry_once(schema_row_min)).id

    # Start execution, two attempts persisted
    r = client.post(f"/execution/{job_id}")
    assert r.status_code == 200
    exec_id = r.json()["execution_id"]

    r = client.get(
        "/execution/executions",
        params={
            "status": "SUCCESS",
            "started_after": "2000-01-01T00:00:00",
            "started_before": "2100-01-01T00:00:00",
            "sort_by": "started_at",
            "order": "desc",
            "limit": 50,
            "offset": 0,
        },
    )
    assert r.status_code == 200
    data = r.json()["data"]
    assert any(row["id"] == exec_id for row in data)

    # Detail shows two attempts ordered by attempt_index ascending
    r = client.get(f"/execution/executions/{exec_id}")
    assert r.status_code == 200
    det = r.json()
    attempts = det["attempts"]
    assert [a["status"] for a in attempts] == ["FAILED", "SUCCESS"]
    assert [a["attempt_index"] for a in attempts] == [1, 2]
