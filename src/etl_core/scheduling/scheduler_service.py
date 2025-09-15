from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
import os
from typing import Any, Dict, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from etl_core.job_execution.job_execution_handler import (
    JobExecutionHandler,
    ExecutionAlreadyRunning,
)
from etl_core.persistence.handlers.job_handler import JobHandler
from etl_core.persistence.handlers.schedule_handler import (
    ScheduleHandler,
    ScheduleNotFoundError,
)
from etl_core.persistence.table_definitions import ScheduleTable, TriggerType


_DEFAULT_SYNC_INTERVAL_SECONDS = 30
_DISABLE_SENTINELS = {
    "0",
    "off",
    "false",
    "disable",
    "disabled",
    "none",
    "no",
}
_NOT_SET = object()


@dataclass
class _Deps:
    schedules: ScheduleHandler
    jobs: JobHandler
    executor: JobExecutionHandler


class SchedulerService:
    """
    Orchestrates AsyncIOScheduler and keeps APS jobs in sync with persisted
    schedules. Provides pause, resume, delete and run-now operations.
    """

    _instance: Optional["SchedulerService"] = None

    def __init__(self, deps: Optional[_Deps] = None) -> None:
        self._log = logging.getLogger("etl_core.scheduler")
        self._deps = deps or _Deps(
            schedules=ScheduleHandler(), jobs=JobHandler(), executor=JobExecutionHandler()
        )
        self._scheduler: Optional[AsyncIOScheduler] = None
        self._started = False

    # ----- Singleton -----
    @classmethod
    def instance(cls) -> "SchedulerService":
        if cls._instance is None:
            cls._instance = SchedulerService()
        return cls._instance

    # ----- Lifecycle -----
    def start(self, sync_seconds: Any = _NOT_SET) -> None:
        """
        Start the scheduler and optionally enable periodic DB -> APS sync.

        Configure via `sync_seconds` argument or env var `ETL_SCHEDULES_SYNC_SECONDS`.
        Set to 0 or one of: off,false,disable,disabled to disable the sync job.
        Default is 30 seconds if not provided.
        """
        if not self._started:
            self._log.info("Starting AsyncIOScheduler")
            scheduler = self._ensure_scheduler()
            scheduler.start()
            self._started = True
            # load DB jobs
            self.sync_from_db()
            # periodically resync from DB in case of out-of-process changes (e.g., CLI)
            try:
                interval = _resolve_sync_interval_seconds(sync_seconds)
                if interval is not None:
                    scheduler.add_job(
                        self.sync_from_db,
                        trigger=IntervalTrigger(seconds=interval),
                        id="__schedules_sync__",
                        replace_existing=True,
                        coalesce=True,
                    )
                    self._log.info(
                        "Scheduled periodic DB sync every %s seconds", interval
                    )
                else:
                    self._log.info("Periodic DB sync disabled")
            except Exception:
                self._log.exception("Failed to schedule periodic DB sync")

    def pause_all(self) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            return
        for job in scheduler.get_jobs() or []:
            try:
                scheduler.pause_job(job.id)
            except Exception:  # noqa: BLE001
                self._log.exception("Failed to pause job %s", job.id)

    async def shutdown_gracefully(self) -> None:
        """
        Pause all schedules and wait for running jobs to complete.
        """
        self._log.info("Graceful shutdown: pausing schedules and waiting for jobs")
        self.pause_all()
        if self._scheduler is not None:
            # shutdown can block; place into a thread to avoid blocking loop
            await asyncio.to_thread(self._scheduler.shutdown, True)
        self._scheduler = None
        self._started = False
        self._log.info("Scheduler fully shut down")

    # ----- DB <-> APS sync -----
    def sync_from_db(self) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            self._log.debug("sync_from_db skipped; scheduler not initialized")
            return

        schedules = self._deps.schedules.list()
        existing_ids = {s.id for s in schedules}

        # remove stale jobs
        for job in scheduler.get_jobs() or []:
            if job.id not in existing_ids:
                self._log.info("Removing stale scheduled job %s", job.id)
                scheduler.remove_job(job.id)

        # add/update current
        for sch in schedules:
            self._upsert_aps_job(sch)

    def _upsert_aps_job(self, sch: ScheduleTable) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            self._log.debug("Skipping upsert; scheduler not initialized")
            return

        trigger = self._to_trigger(sch.trigger_type, sch.trigger_args)
        fn = self._job_callable(sch.id)
        replace_existing = True
        kwargs: Dict[str, Any] = {}
        try:
            scheduler.add_job(
                fn,
                trigger=trigger,
                id=sch.id,
                name=f"{sch.name}:{sch.job_id}",
                replace_existing=replace_existing,
                kwargs=kwargs,
                coalesce=True,
                max_instances=1,
            )
            if sch.is_paused:
                scheduler.pause_job(sch.id)
            self._log.info(
                "Scheduled '%s' (job_id=%s) with %s trigger (paused=%s)",
                sch.name,
                sch.job_id,
                sch.trigger_type,
                sch.is_paused,
            )
        except Exception:  # noqa: BLE001
            self._log.exception("Failed to upsert APS job for schedule %s", sch.id)

    def _to_trigger(self, ttype: TriggerType, args: Dict[str, Any]):
        if ttype == TriggerType.INTERVAL:
            return IntervalTrigger(**args)
        if ttype == TriggerType.CRON:
            return CronTrigger(**args)
        if ttype == TriggerType.DATE:
            return DateTrigger(**args)
        raise ValueError(f"Unsupported trigger type: {ttype}")

    def _job_callable(self, schedule_id: str):
        async def _runner():
            await self._run_schedule(schedule_id)

        return _runner

    async def _run_schedule(self, schedule_id: str) -> None:
        # fetch schedule fresh from DB to use latest state (off the event loop)
        sch = await asyncio.to_thread(self._deps.schedules.get, schedule_id)
        if sch is None:
            self._log.warning("Schedule %s not found; removing from APS", schedule_id)
            try:
                scheduler = self._scheduler
                if scheduler is not None:
                    scheduler.remove_job(schedule_id)
            except Exception:  # noqa: BLE001
                pass
            return

        # run the job in a thread to avoid blocking the loop
        try:
            runtime_job = await asyncio.to_thread(
                self._deps.jobs.load_runtime_job, sch.job_id
            )
        except Exception as exc:  # noqa: BLE001
            self._log.exception(
                "Failed to load job id '%s' for schedule %s: %s",
                sch.job_id,
                schedule_id,
                exc,
            )
            return

        env = sch.context
        self._log.info("Executing scheduled job id '%s' (env=%s)", sch.job_id, env)
        try:
            await asyncio.to_thread(self._deps.executor.execute_job, runtime_job, env)
        except ExecutionAlreadyRunning:
            self._log.warning(
                "Scheduled job id '%s' already running; skipping trigger", sch.job_id
            )
            return
        self._log.info("Finished scheduled job id '%s'", sch.job_id)

    # ----- Public operations used by API/CLI -----
    def add_schedule(self, sch: ScheduleTable) -> None:
        self._upsert_aps_job(sch)

    def remove_schedule(self, schedule_id: str) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            return
        try:
            scheduler.remove_job(schedule_id)
        except Exception:  # noqa: BLE001
            self._log.exception("Failed to remove schedule %s from APS", schedule_id)

    def pause_schedule(self, schedule_id: str) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            return
        try:
            scheduler.pause_job(schedule_id)
        except Exception:  # noqa: BLE001
            self._log.exception("Failed to pause schedule %s", schedule_id)

    def resume_schedule(self, schedule_id: str) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            return
        try:
            scheduler.resume_job(schedule_id)
        except Exception:  # noqa: BLE001
            self._log.exception("Failed to resume schedule %s", schedule_id)

    async def run_now(self, schedule_id: str) -> None:
        await self._run_schedule(schedule_id)

    def _ensure_scheduler(self) -> AsyncIOScheduler:
        scheduler = self._scheduler
        if scheduler is not None:
            scheduler_loop = getattr(scheduler, "_eventloop", None)
            if scheduler_loop is None or scheduler_loop.is_closed():
                scheduler = None
        if scheduler is None:
            scheduler = AsyncIOScheduler()
            self._scheduler = scheduler
        return scheduler


def parse_sync_interval(raw: Any) -> Optional[int]:
    """Normalize sync interval values from config/env overrides."""
    if raw is None:
        return _DEFAULT_SYNC_INTERVAL_SECONDS

    if isinstance(raw, str):
        normalized = raw.strip()
        if not normalized:
            return _DEFAULT_SYNC_INTERVAL_SECONDS
        lowered = normalized.lower()
        if lowered in _DISABLE_SENTINELS:
            return None
        try:
            value = int(normalized)
        except Exception:
            return _DEFAULT_SYNC_INTERVAL_SECONDS
        return value if value > 0 else None

    if isinstance(raw, int):
        return raw if raw > 0 else None

    return _DEFAULT_SYNC_INTERVAL_SECONDS


def _resolve_sync_interval_seconds(explicit: Any = _NOT_SET) -> Optional[int]:
    if explicit is not _NOT_SET:
        return parse_sync_interval(explicit)

    return parse_sync_interval(os.getenv("ETL_SCHEDULES_SYNC_SECONDS"))
