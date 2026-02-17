from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from etl_core.api.routers import execution as R
from etl_core.context.environment import Environment
from etl_core.persistence.errors import PersistNotFoundError


def test_start_execution_uses_background_handler():
    recorded: dict[str, object] = {}

    class DummyJobHandler:
        @staticmethod
        def load_runtime_job(job_id: str):
            recorded["job_id"] = job_id
            return SimpleNamespace(id=job_id, name="demo")

    class DummyExecutionHandler:
        @staticmethod
        def start_job_background(runtime_job, environment=None):
            recorded["runtime_job"] = runtime_job
            recorded["environment"] = environment
            return SimpleNamespace(id="exec-1", max_attempts=3)

    body = R.StartExecutionBody(environment=Environment.DEV)
    out = R.start_execution(
        "job-1",
        job_handler=DummyJobHandler(),
        execution_handler=DummyExecutionHandler(),
        body=body,
    )

    assert out["status"] == "started"
    assert out["execution_id"] == "exec-1"
    assert out["environment"] == "DEV"
    assert recorded["job_id"] == "job-1"
    assert recorded["environment"] == Environment.DEV


def test_start_execution_returns_404_for_missing_job():
    class DummyJobHandler:
        @staticmethod
        def load_runtime_job(_job_id: str):
            raise PersistNotFoundError("missing")

    class DummyExecutionHandler:
        @staticmethod
        def start_job_background(_runtime_job, environment=None):
            return SimpleNamespace(id="should-not-run", max_attempts=1)

    with pytest.raises(HTTPException) as ei:
        R.start_execution(
            "missing",
            job_handler=DummyJobHandler(),
            execution_handler=DummyExecutionHandler(),
            body=None,
        )

    assert ei.value.status_code == 404


def test_get_execution_progress_success(monkeypatch):
    now = datetime.now()
    snapshot = {
        "execution_id": "exec-1",
        "job_id": "job-1",
        "job_name": "demo",
        "environment": "DEV",
        "status": "RUNNING",
        "started_at": now,
        "finished_at": None,
        "active_attempt": 1,
        "last_error": None,
        "rows_received_total": 10,
        "rows_forwarded_total": 8,
        "log_path": "/tmp/demo.log",
        "updated_at": now,
        "components": [
            {
                "component_id": "c1",
                "component_name": "reader",
                "status": "RUNNING",
                "rows_received": 10,
                "rows_forwarded": 8,
                "error_count": 0,
                "last_event": "emit:out",
                "updated_at": now,
            }
        ],
    }

    class FakeStore:
        def snapshot(self, execution_id: str):
            assert execution_id == "exec-1"
            return snapshot

    monkeypatch.setattr(R, "execution_telemetry_store", lambda: FakeStore())
    out = R.get_execution_progress("exec-1")
    assert out.execution_id == "exec-1"
    assert out.rows_received_total == 10
    assert len(out.components) == 1


def test_get_execution_progress_not_found(monkeypatch):
    class FakeStore:
        @staticmethod
        def snapshot(_execution_id: str):
            return None

    monkeypatch.setattr(R, "execution_telemetry_store", lambda: FakeStore())
    with pytest.raises(HTTPException) as ei:
        R.get_execution_progress("missing")
    assert ei.value.status_code == 404


def test_get_execution_logs_success(monkeypatch):
    class FakeStore:
        @staticmethod
        def snapshot(_execution_id: str):
            return {"execution_id": "exec-1"}

        @staticmethod
        def tail_logs(_execution_id: str, *, tail: int = 200):
            assert tail == 33
            return {
                "lines": ["line 1", "line 2"],
                "log_path": "/tmp/demo.log",
                "redacted": True,
            }

    monkeypatch.setattr(R, "execution_telemetry_store", lambda: FakeStore())
    out = R.get_execution_logs("exec-1", tail=33)
    assert out.execution_id == "exec-1"
    assert out.lines == ["line 1", "line 2"]
    assert out.redacted is True
