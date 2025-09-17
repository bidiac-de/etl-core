from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any, Dict, Tuple

import pytest

from etl_core.scheduling import scheduler_service


class DummyScheduleHandler:
    def list(self) -> list:
        return []


class DummyJobHandler:
    pass


class DummyExecutor:
    pass


class FakeScheduler:
    def __init__(self) -> None:
        self.started = False
        self.jobs: Dict[str, SimpleNamespace] = {}

    def start(self) -> None:
        self.started = True

    def add_job(
        self,
        func: Any,
        trigger: Any,
        id: str,
        name: str | None = None,
        replace_existing: bool = True,
        kwargs: Dict[str, Any] | None = None,
        coalesce: bool = False,
        max_instances: int = 1,
    ) -> SimpleNamespace:
        job = SimpleNamespace(
            id=id,
            func=func,
            trigger=trigger,
            name=name,
            replace_existing=replace_existing,
            kwargs=kwargs or {},
            coalesce=coalesce,
            max_instances=max_instances,
        )
        self.jobs[id] = job
        return job

    def get_jobs(self) -> list:
        return list(self.jobs.values())

    def remove_job(self, job_id: str) -> None:
        self.jobs.pop(job_id, None)

    def pause_job(self, job_id: str) -> None:
        # no-op for tests
        return None

    def resume_job(self, job_id: str) -> None:
        # no-op for tests
        return None


def _make_service(
    monkeypatch: pytest.MonkeyPatch,
) -> Tuple[scheduler_service.SchedulerService, FakeScheduler, Dict[str, Any]]:
    fake_scheduler = FakeScheduler()
    monkeypatch.setattr(scheduler_service, "AsyncIOScheduler", lambda: fake_scheduler)

    captured: Dict[str, Any] = {}

    class CaptureIntervalTrigger:
        def __init__(self, **kwargs: Any) -> None:
            captured["kwargs"] = kwargs

    monkeypatch.setattr(scheduler_service, "IntervalTrigger", CaptureIntervalTrigger)

    deps = scheduler_service._Deps(
        schedules=DummyScheduleHandler(),
        jobs=DummyJobHandler(),
        executor=DummyExecutor(),
    )

    service = scheduler_service.SchedulerService(deps=deps)
    return service, fake_scheduler, captured


def test_start_schedules_default_sync_job(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ETL_SCHEDULES_SYNC_SECONDS", raising=False)
    service, fake_scheduler, captured = _make_service(monkeypatch)

    service.start()

    assert fake_scheduler.started is True
    assert "__schedules_sync__" in fake_scheduler.jobs
    assert captured["kwargs"]["seconds"] == 30


def test_start_respects_override_from_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ETL_SCHEDULES_SYNC_SECONDS", raising=False)
    service, fake_scheduler, captured = _make_service(monkeypatch)

    service.start("45")

    assert fake_scheduler.started is True
    assert captured["kwargs"]["seconds"] == 45


def test_start_can_disable_sync_job(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ETL_SCHEDULES_SYNC_SECONDS", raising=False)
    service, fake_scheduler, captured = _make_service(monkeypatch)

    service.start("off")

    assert fake_scheduler.started is True
    assert "__schedules_sync__" not in fake_scheduler.jobs
    assert "kwargs" not in captured


def test_start_uses_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ETL_SCHEDULES_SYNC_SECONDS", "12")
    service, fake_scheduler, captured = _make_service(monkeypatch)

    service.start()

    assert fake_scheduler.started is True
    assert captured["kwargs"]["seconds"] == 12


@pytest.mark.asyncio
async def test_run_schedule_logs_when_job_already_running(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    schedule = SimpleNamespace(
        id="schedule-1",
        name="Demo",
        job_id="job-1",
        context="dev",
        trigger_type=None,
        trigger_args={"seconds": 10},
        is_paused=False,
    )

    runtime_job = SimpleNamespace(id="job-1", name="demo-job")

    class BlockingScheduleHandler:
        def list(self) -> list:
            return [schedule]

        def get(self, schedule_id: str) -> SimpleNamespace:
            assert schedule_id == schedule.id
            return schedule

    class RuntimeJobHandler:
        def load_runtime_job(self, job_id: str) -> SimpleNamespace:
            assert job_id == runtime_job.id
            return runtime_job

    class BusyExecutor:
        def __init__(self) -> None:
            self.calls = 0

        def execute_job(self, job: SimpleNamespace, env: str | None = None) -> None:
            self.calls += 1
            raise scheduler_service.ExecutionAlreadyRunning("already running")

    deps = scheduler_service._Deps(
        schedules=BlockingScheduleHandler(),
        jobs=RuntimeJobHandler(),
        executor=BusyExecutor(),
    )
    service = scheduler_service.SchedulerService(deps=deps)

    async def immediate_to_thread(func: Any, *args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    monkeypatch.setattr(scheduler_service.asyncio, "to_thread", immediate_to_thread)

    caplog.set_level(logging.WARNING, logger="etl_core.scheduler")

    await service._run_schedule(schedule.id)

    assert "already running; skipping trigger" in caplog.text
    assert deps.executor.calls == 1
