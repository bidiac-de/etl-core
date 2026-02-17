from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace

import pytest

from etl_core.context.credentials import Credentials
from etl_core.context.credentials_mapping_context import CredentialsMappingContext
from etl_core.context.environment import Environment
from etl_core.job_execution.job_execution_handler import (
    ExecutionAlreadyRunning,
    JobExecutionHandler,
)
from etl_core.job_execution.runtimejob import JobExecution, RuntimeJob


class DummyExecRecords:
    """Fake persistence that always raises to cover exception logging branches."""

    def create_execution(self, *_, **__):
        raise RuntimeError("persist fail")

    def start_attempt(self, *_, **__):
        raise RuntimeError("persist start fail")

    def finish_attempt(self, *_, **__):
        raise RuntimeError("persist finish fail")


@pytest.mark.asyncio
async def test_persist_attempt_branches_logged(caplog):
    handler = JobExecutionHandler()
    job = RuntimeJob(
        name="dummy",
        num_of_retries=0,
        file_logging=False,
        strategy_type="row",
        components=[],
        metadata={},
    )
    execution = JobExecution(job)
    execution.start_attempt()

    handler._exec_records_handler = DummyExecRecords()  # type: ignore[attr-defined]
    caplog.set_level(logging.ERROR)

    handler._persist_attempt_start(execution)
    handler._persist_attempt_finish(execution, status="SUCCESS", error=None)
    handler._persist_attempt_finish(execution, status="FAILED", error="x")

    assert "persist" in caplog.text.lower()


def test_extract_exception_and_should_retry():
    handler = JobExecutionHandler()
    inner = Exception("x")
    group = ExceptionGroup("root", [inner])
    assert handler._extract_exception(group) is inner
    assert str(handler._extract_exception(RuntimeError("y"))) == "y"

    dummy_exec = SimpleNamespace(
        retry_strategy=SimpleNamespace(should_retry=lambda i: i < 1)
    )
    assert handler._should_retry(dummy_exec, 0) is True
    assert handler._should_retry(dummy_exec, 1) is False
    assert handler._should_retry(dummy_exec, 2) is False


@pytest.mark.asyncio
async def test_maybe_wait_before_retry_sleeps(monkeypatch):
    handler = JobExecutionHandler()
    dummy_exec = SimpleNamespace(
        retry_strategy=SimpleNamespace(next_delay=lambda _i: 0.01)
    )
    called = {"sleep": False}

    async def fake_sleep(delay: float) -> None:  # noqa: D401
        called["sleep"] = True
        assert delay == 0.01

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    await handler._maybe_wait_before_retry(dummy_exec, 1)
    assert called["sleep"] is True


def test_normalize_environment_variants():
    handler = JobExecutionHandler()
    assert handler._normalize_environment(Environment.DEV) == "DEV"
    assert handler._normalize_environment("DEV") == "DEV"
    assert handler._normalize_environment("custom") == "CUSTOM"
    assert handler._normalize_environment(None) is None


def test_begin_execution_and_release_guard():
    handler = JobExecutionHandler()

    job = RuntimeJob(
        name="job",
        num_of_retries=0,
        file_logging=False,
        strategy_type="row",
        components=[],
        metadata={},
    )

    handler._exec_records_handler = DummyExecRecords()  # type: ignore[attr-defined]
    _ = handler._begin_execution(job, None)
    assert (job.id, None) in handler._running_jobs

    with pytest.raises(ExecutionAlreadyRunning):
        handler._begin_execution(job, None)

    handler._release_execution(job.id, None)
    assert (job.id, None) not in handler._running_jobs
    handler._exec_records_handler = SimpleNamespace(
        create_execution=lambda **_: None
    )  # type: ignore[attr-defined]
    execution2 = handler._begin_execution(job, None)
    assert (execution2.job.id, None) in handler._running_jobs
    handler._release_execution(job.id, None)


def test_concurrent_different_environments():
    """Same job can run in DEV and PROD simultaneously."""
    handler = JobExecutionHandler()
    handler._exec_records_handler = SimpleNamespace(
        create_execution=lambda **_: None
    )  # type: ignore[attr-defined]

    job = RuntimeJob(
        name="concurrent-job",
        num_of_retries=0,
        file_logging=False,
        strategy_type="row",
        components=[],
        metadata={},
    )

    _ = handler._begin_execution(job, "DEV")
    assert (job.id, "DEV") in handler._running_jobs

    _ = handler._begin_execution(job, "PROD")
    assert (job.id, "PROD") in handler._running_jobs

    # Same job same env should still fail
    with pytest.raises(ExecutionAlreadyRunning):
        handler._begin_execution(job, "DEV")

    handler._release_execution(job.id, "DEV")
    assert (job.id, "DEV") not in handler._running_jobs
    assert (job.id, "PROD") in handler._running_jobs

    handler._release_execution(job.id, "PROD")
    assert (job.id, "PROD") not in handler._running_jobs


def test_cleanup_after_execution_calls_cleanup(monkeypatch):
    called: dict[str, bool] = {}

    class DummyComp:
        def __init__(self, name: str) -> None:
            self.name = name

        def cleanup_after_execution(self) -> None:
            called[self.name] = True

    class DummyPool:
        @staticmethod
        def close_idle_pools():
            return {"sql": True, "mongo": False}

    handler = JobExecutionHandler()
    job = SimpleNamespace(components=[DummyComp("c1"), DummyComp("c2")])
    execution = SimpleNamespace(job=job)

    monkeypatch.setattr(
        "etl_core.components.databases.pool_registry.ConnectionPoolRegistry.instance",
        lambda: DummyPool(),
    )
    handler._cleanup_after_execution(execution)
    assert called == {"c1": True, "c2": True}


def test_prepare_components_resolves_ctx_templates_and_calls_prepare(monkeypatch):
    class DummyRepo:
        def get_by_id(self, credentials_id: str):
            if credentials_id != "cred-prod":
                return None
            return (
                Credentials(
                    name="prod",
                    user="prod_user",
                    host="h",
                    port=5432,
                    database="d",
                    password="pw",
                ),
                credentials_id,
            )

    class DummyComp:
        model_fields = {"context_id": None, "value": None}

        def __init__(self) -> None:
            self.name = "dummy"
            self.context_id = "ctx-1"
            self.value = "${ctx.user}"
            self._ctx = CredentialsMappingContext(
                name="mapping",
                environment=Environment.DEV,
                credentials_ids={"PROD": "cred-prod"},
            )
            self.prepared: list[Environment | None] = []

        def get_resolved_context(self):
            return self._ctx

        def prepare_for_execution(self, environment: Environment | None) -> None:
            self.prepared.append(environment)

    monkeypatch.setattr(
        "etl_core.singletons.credentials_handler",
        lambda: DummyRepo(),
    )

    handler = JobExecutionHandler()
    comp = DummyComp()
    job = SimpleNamespace(components=[comp])
    handler._prepare_comps_for_execution(job, Environment.PROD)

    assert comp.value == "prod_user"
    assert comp.prepared == [Environment.PROD]


@pytest.mark.asyncio
async def test_broadcast_and_worker_cancel():
    handler = JobExecutionHandler()

    class DummyComp:
        name = "dummy"
        id = "cid"
        prev_components: list[object] = []
        next_components: list[object] = []

        async def execute(self, payload, metrics):  # type: ignore[no-untyped-def]
            if False:  # keep as generator
                yield None
            yield SimpleNamespace(port="out", payload=123)

    class DummyMetrics:
        def __init__(self) -> None:
            self.status = "PENDING"
            self.error_count = 0

        def update_processing_time(self) -> None:
            self.updated = True

        def set_started(self) -> None:
            self.started = True

    comp = DummyComp()
    metrics = DummyMetrics()
    q: asyncio.Queue = asyncio.Queue()
    edges = {"out": [(q, "in", False)]}

    class Sentinel:
        pass

    execution = SimpleNamespace(
        id="exec-test",
        latest_attempt=lambda: SimpleNamespace(current_tasks={}),
        sentinels={comp.id: Sentinel()},
    )

    with pytest.raises(TypeError):
        await handler._worker(execution, comp, [], edges, metrics, {})

    assert q.qsize() == 1
