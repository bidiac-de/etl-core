from __future__ import annotations

from datetime import datetime
from typing import Tuple

import pytest
from sqlmodel import SQLModel, create_engine, Session, select

import etl_core.job_execution.runtimejob as runtimejob_module
from etl_core.components.runtime_state import RuntimeState
from etl_core.components.stubcomponents import StubComponent
from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from etl_core.persistance.handlers.execution_records_handler import (
    ExecutionRecordsHandler,
)
from etl_core.persistance.table_definitions import ExecutionTable, ExecutionAttemptTable
from tests.helpers import runtime_job_from_config

# Ensure tests can build the "test" component by name like in your existing suite
runtimejob_module.TestComponent = StubComponent


@pytest.fixture(scope="function")
def mem_engine():
    """
    Dedicated in-memory SQLite engine with tables created fresh for every test.
    We deliberately do NOT enable foreign key enforcement to keep the setup
    lightweight; ExecutionTable.job_id references JobTable, but we don't need
    to insert JobTable rows for these persistence-focused tests.
    """
    eng = create_engine("sqlite://")
    SQLModel.metadata.create_all(eng)
    return eng


@pytest.fixture()
def records_handler(mem_engine) -> ExecutionRecordsHandler:
    # Inject our engine into handler so it uses the test DB
    return ExecutionRecordsHandler(engine_=mem_engine)


@pytest.fixture()
def patched_exec_handler(
    records_handler: ExecutionRecordsHandler,
) -> JobExecutionHandler:
    """
    Real JobExecutionHandler, but with its _exec_records_handler replaced so
    that execution/attempt persistence hits the test DB.
    """
    handler = JobExecutionHandler()
    handler._exec_records_handler = records_handler  # type: ignore[attr-defined]
    return handler


def _simple_success_job(schema_row_min) -> dict:
    return {
        "name": "PersistSuccess",
        "num_of_retries": 0,
        "file_logging": False,
        "metadata": {"user_id": 1, "timestamp": datetime.now()},
        "strategy_type": "row",
        "components": [
            {
                "name": "c1",
                "comp_type": "test",
                "description": "",
                "routes": {"out": []},
                "out_port_schemas": {"out": schema_row_min},
            }
        ],
    }


def _simple_fail_job(schema_row_min) -> dict:
    return {
        "name": "PersistFail",
        "num_of_retries": 0,
        "file_logging": False,
        "metadata": {"user_id": 1, "timestamp": datetime.now()},
        "strategy_type": "row",
        "components": [
            {
                "name": "c1",
                "comp_type": "failtest",
                "description": "fails immediately",
                "routes": {"out": []},
                "out_port_schemas": {"out": schema_row_min},
            }
        ],
    }


def _retry_once_job(schema_row_min) -> dict:
    return {
        "name": "PersistRetryOnce",
        "num_of_retries": 1,
        "file_logging": False,
        "metadata": {"user_id": 1, "timestamp": datetime.now()},
        "strategy_type": "row",
        "components": [
            {
                "name": "c1",
                "comp_type": "stub_fail_once",
                "description": "first attempt fails, then succeeds",
                "routes": {"out": []},
                "out_port_schemas": {"out": schema_row_min},
            }
        ],
    }


def _fetch_exec_and_attempts(
    engine, execution_id: str
) -> Tuple[ExecutionTable, list[ExecutionAttemptTable]]:
    with Session(engine) as s:
        exec_row = s.get(ExecutionTable, execution_id)
        atts = list(
            s.exec(
                select(ExecutionAttemptTable)
                .where(ExecutionAttemptTable.execution_id == execution_id)
                .order_by(ExecutionAttemptTable.attempt_index.asc())
            )
        )
        assert exec_row is not None, "Execution row should exist"
        return exec_row, atts


def test_persist_success_single_attempt(
    patched_exec_handler, records_handler, mem_engine, schema_row_min
):
    """
    A successful one-attempt run must persist:
      - execution row with status SUCCESS and finished_at set
      - exactly one attempt row with status SUCCESS and attempt_index==1
    """
    handler = patched_exec_handler
    cfg = _simple_success_job(schema_row_min)
    job = runtime_job_from_config(cfg)

    execution = handler.execute_job(job)
    exec_row, attempts = _fetch_exec_and_attempts(mem_engine, execution.id)

    # Execution persisted as SUCCESS with timestamps
    assert exec_row.status == "SUCCESS"
    assert exec_row.error is None
    assert exec_row.started_at is not None
    assert exec_row.finished_at is not None

    assert len(attempts) == 1
    first = attempts[0]
    assert first.attempt_index == 1
    assert first.status == "SUCCESS"
    assert first.error is None
    assert first.started_at is not None
    assert first.finished_at is not None

    mh = handler.job_info.metrics_handler
    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS


def test_persist_failure_no_retry(
    patched_exec_handler, records_handler, mem_engine, schema_row_min
):
    """
    A failing run (no retries) must persist:
      - execution row with status FAILED and error message
      - a single attempt row with status FAILED and matching error
    """
    handler = patched_exec_handler
    cfg = _simple_fail_job(schema_row_min)
    job = runtime_job_from_config(cfg)

    execution = handler.execute_job(job)
    exec_row, attempts = _fetch_exec_and_attempts(mem_engine, execution.id)

    assert exec_row.status == "FAILED"
    assert exec_row.error is not None and "fail stubcomponent failed" in exec_row.error
    assert exec_row.finished_at is not None

    assert len(attempts) == 1
    att = attempts[0]
    assert att.attempt_index == 1
    assert att.status == "FAILED"
    assert att.error is not None and "fail stubcomponent failed" in att.error


def test_persist_retry_then_success(
    patched_exec_handler, records_handler, mem_engine, schema_row_min
):
    """
    A job that fails once then succeeds must persist:
      - execution row with final status SUCCESS
      - two attempts: [FAILED, SUCCESS] with ordered attempt_index
    """
    handler = patched_exec_handler
    cfg = _retry_once_job(schema_row_min)
    job = runtime_job_from_config(cfg)

    execution = handler.execute_job(job)
    exec_row, attempts = _fetch_exec_and_attempts(mem_engine, execution.id)

    assert exec_row.status == "SUCCESS"
    assert len(attempts) == 2
    first, second = attempts

    # First attempt failed and was finished with FAILED (and error)
    assert first.attempt_index == 1
    assert first.status == "FAILED"
    assert first.error

    # Second attempt succeeded and was finished with SUCCESS (no error)
    assert second.attempt_index == 2
    assert second.status == "SUCCESS"
    assert second.error is None


def test_handler_queries_filter_and_order(
    patched_exec_handler, records_handler, mem_engine, schema_row_min
):
    """
    Run two executions (one FAIL, one SUCCESS) and verify list/get helpers:
      - list_executions filtering by status and environment
      - ordering by started_at DESC
      - get_execution returns attempts ordered by attempt_index ASC
    """
    handler = patched_exec_handler

    ok_job = runtime_job_from_config(_simple_success_job(schema_row_min))
    fail_job = runtime_job_from_config(_simple_fail_job(schema_row_min))

    ok_exec = handler.execute_job(ok_job)
    fail_exec = handler.execute_job(fail_job)

    # list_executions: filter by status
    ok_rows, ok_total = records_handler.list_executions(status="SUCCESS")
    fail_rows, fail_total = records_handler.list_executions(status="FAILED")
    assert ok_total >= 1 and any(r.id == ok_exec.id for r in ok_rows)
    assert fail_total >= 1 and any(r.id == fail_exec.id for r in fail_rows)

    all_rows, _ = records_handler.list_executions(order="desc")
    assert len(all_rows) >= 2
    assert all_rows[0].started_at >= all_rows[-1].started_at

    exec_row, attempts = records_handler.get_execution(ok_exec.id)
    assert exec_row is not None
    assert attempts == sorted(attempts, key=lambda a: a.attempt_index)


def test_attempt_rows_have_run_timestamps(
    patched_exec_handler, mem_engine, schema_row_min
):
    """
    Smoke test for attempt timestamps set by start_attempt(...) and finish_attempt(...).
    We don't inspect values, only that both start & finish stamps exist.
    """
    handler = patched_exec_handler
    job = runtime_job_from_config(_simple_success_job(schema_row_min))
    execution = handler.execute_job(job)

    with Session(mem_engine) as s:
        rows = list(
            s.exec(
                select(ExecutionAttemptTable).where(
                    ExecutionAttemptTable.execution_id == execution.id
                )
            )
        )
    assert rows and rows[0].started_at is not None and rows[0].finished_at is not None
