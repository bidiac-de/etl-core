from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import patch

from typer.testing import CliRunner

from etl_core.context.environment import Environment
from etl_core.persistence.errors import PersistNotFoundError
from etl_core.api.cli.cli_app import app
from contextlib import contextmanager


def runner() -> CliRunner:
    return CliRunner(mix_stderr=False)


class _FakeExecClient:
    def __init__(self) -> None:
        self.last_list_kwargs: Dict[str, Any] = {}
        self.started: List[Dict[str, Any]] = []

    def start(
        self, job_id: str, environment: Optional[Environment] = None
    ) -> Dict[str, Any]:
        payload = {
            "job_id": job_id,
            "status": "started",
            "execution_id": "exec-ok-1",
            "max_attempts": 1,
            "environment": environment.value if environment else None,
        }
        self.started.append(payload)
        return payload

    def list_executions(
        self,
        *,
        job_id: Optional[str] = None,
        status: Optional[str] = None,
        environment: Optional[str] = None,
        started_after: Optional[str] = None,
        started_before: Optional[str] = None,
        sort_by: str = "started_at",
        order: str = "desc",
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        self.last_list_kwargs = {
            "job_id": job_id,
            "status": status,
            "environment": environment,
            "started_after": started_after,
            "started_before": started_before,
            "sort_by": sort_by,
            "order": order,
            "limit": limit,
            "offset": offset,
        }
        return {
            "data": [
                {
                    "id": "exec-ok-1",
                    "job_id": job_id or "job-1",
                    "environment": environment,
                    "status": status or "SUCCESS",
                    "error": None,
                    "started_at": "2025-09-13T12:00:00",
                    "finished_at": "2025-09-13T12:05:00",
                }
            ]
        }

    def get(self, execution_id: str) -> Dict[str, Any]:
        if execution_id == "missing":
            raise PersistNotFoundError("not found")
        return {
            "execution": {
                "id": execution_id,
                "job_id": "job-1",
                "environment": "TEST",
                "status": "SUCCESS",
                "error": None,
                "started_at": "2025-09-13T12:00:00",
                "finished_at": "2025-09-13T12:05:00",
            },
            "attempts": [
                {
                    "id": "att-1",
                    "execution_id": execution_id,
                    "attempt_index": 1,
                    "status": "SUCCESS",
                    "error": None,
                    "started_at": "2025-09-13T12:00:00",
                    "finished_at": "2025-09-13T12:05:00",
                }
            ],
        }

    def attempts(self, execution_id: str) -> List[Dict[str, Any]]:
        if execution_id == "missing":
            raise PersistNotFoundError("not found")
        return [
            {
                "id": "att-1",
                "execution_id": execution_id,
                "attempt_index": 1,
                "status": "SUCCESS",
                "error": None,
                "started_at": "2025-09-13T12:00:00",
                "finished_at": "2025-09-13T12:05:00",
            }
        ]


@contextmanager
def patched_pick_exec() -> _FakeExecClient:
    with patch("etl_core.api.cli.commands.execution.pick_clients") as pick:
        fake = _FakeExecClient()
        pick.side_effect = lambda: (SimpleNamespace(), fake, SimpleNamespace())
        try:
            yield fake
        finally:
            pass


def test_cli_execution_list_success_forwards_filters() -> None:
    with patched_pick_exec() as fake:
        res = runner().invoke(
            app,
            [
                "execution",
                "list",
                "--status",
                "SUCCESS",
                "--environment",
                "TEST",
                "--started-after",
                "2025-09-13T12:00:00",
                "--started-before",
                "2025-09-13T13:00:00",
                "--sort-by",
                "started_at",
                "--order",
                "asc",
                "--limit",
                "10",
                "--offset",
                "5",
            ],
        )
        assert res.exit_code == 0, res.stdout

        payload = json.loads(res.stdout)
        assert list(payload.keys()) == ["data"]
        assert payload["data"][0]["id"] == "exec-ok-1"

        assert fake.last_list_kwargs == {
            "job_id": None,
            "status": "SUCCESS",
            "environment": "TEST",
            "started_after": "2025-09-13T12:00:00",
            "started_before": "2025-09-13T13:00:00",
            "sort_by": "started_at",
            "order": "asc",
            "limit": 10,
            "offset": 5,
        }


def test_cli_execution_get_detail() -> None:
    with patched_pick_exec():
        res = runner().invoke(app, ["execution", "get", "exec-123"])
        assert res.exit_code == 0, res.stdout

        body = json.loads(res.stdout)
        assert "execution" in body and "attempts" in body
        assert body["execution"]["id"] == "exec-123"
        assert body["attempts"][0]["attempt_index"] == 1

        res_missing = runner().invoke(app, ["execution", "get", "missing"])
        assert res_missing.exit_code == 1
        assert "Execution with ID missing not found" in res_missing.stdout


def test_cli_execution_attempts() -> None:
    with patched_pick_exec():
        res = runner().invoke(app, ["execution", "attempts", "exec-xyz"])
        assert res.exit_code == 0, res.stdout

        rows = json.loads(res.stdout)
        assert isinstance(rows, list) and rows
        assert rows[0]["execution_id"] == "exec-xyz"

        res_missing = runner().invoke(app, ["execution", "attempts", "missing"])
        assert res_missing.exit_code == 1
        assert "Execution with ID missing not found" in res_missing.stdout


def test_cli_execution_start_with_and_without_environment() -> None:
    with patched_pick_exec():
        res1 = runner().invoke(app, ["execution", "start", "job-1"])
        assert res1.exit_code == 0, res1.stdout
        payload1 = json.loads(res1.stdout)
        assert payload1["status"] == "started"
        assert payload1["environment"] is None

        res2 = runner().invoke(
            app, ["execution", "start", "job-1", "--environment", "test"]
        )
        assert res2.exit_code == 0, res2.stdout
        payload2 = json.loads(res2.stdout)
        assert payload2["status"] == "started"
        assert payload2["environment"] == "TEST"
