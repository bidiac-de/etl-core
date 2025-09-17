from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
import os
import re
from typing import Any, Dict, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.jobstores.base import JobLookupError

from etl_core.job_execution.job_execution_handler import (
    JobExecutionHandler,
    ExecutionAlreadyRunning,
)
from etl_core.persistence.handlers.job_handler import JobHandler
from etl_core.persistence.handlers.schedule_handler import ScheduleHandler
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

# Reserve IDs for internal/housekeeping jobs that should never be removed by DB sync.
_SYNC_JOB_ID = "__schedules_sync__"
_INTERNAL_JOB_IDS = {_SYNC_JOB_ID}


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
        if deps is None:
            from etl_core.singletons import (
                schedule_handler as _schedule_handler_singleton,
            )

            deps = _Deps(
                schedules=_schedule_handler_singleton(),
                jobs=JobHandler(),
                executor=JobExecutionHandler(),
            )
        self._deps = deps
        self._scheduler: Optional[AsyncIOScheduler] = None
        self._started = False

    @classmethod
    def instance(cls) -> "SchedulerService":
        if cls._instance is None:
            cls._instance = SchedulerService()
        return cls._instance

    def start(self, sync_seconds: Any = _NOT_SET) -> None:
        """
        Start the scheduler and optionally enable periodic DB -> APS sync.

        Configure via `sync_seconds` argument or env var `ETL_SCHEDULES_SYNC_SECONDS`.
        Set to 0 or one of: off,false,disable,disabled to disable the sync job.
        Default is 30 seconds if not provided.
        """
        if self._started:
            return
        self._log.info("Starting AsyncIOScheduler")
        scheduler = self._ensure_scheduler()
        scheduler.start()
        self._started = True

        # Initial sync before scheduling the periodic sync task.
        self.sync_from_db()
        interval = _resolve_sync_interval_seconds(sync_seconds)
        if interval is None:
            self._log.info("Periodic DB sync disabled")
            return

        try:
            scheduler.add_job(
                self.sync_from_db,
                trigger=IntervalTrigger(seconds=interval),
                id=_SYNC_JOB_ID,
                replace_existing=True,
                coalesce=True,
                max_instances=1,
            )
        except Exception:
            self._log.exception("Failed to schedule periodic DB sync")
            raise

        self._log.info("Scheduled periodic DB sync every %s seconds", interval)

    def pause_all(self) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            return
        for job in scheduler.get_jobs() or []:
            try:
                scheduler.pause_job(job.id)
            except JobLookupError:
                self._log.warning(
                    "Job %s disappeared before it could be paused", job.id
                )
            except Exception:
                self._log.exception("Failed to pause job %s", job.id)

    async def shutdown_gracefully(self) -> None:
        """
        Pause all schedules and wait for running jobs to complete.
        """
        self._log.info("Graceful shutdown: pausing schedules and waiting for jobs")
        self.pause_all()
        if self._scheduler is not None:
            # Wait=True to let running jobs finish.
            await asyncio.to_thread(self._scheduler.shutdown, True)
        self._scheduler = None
        self._started = False
        self._log.info("Scheduler fully shut down")

    def sync_from_db(self) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            self._log.debug("sync_from_db skipped; scheduler not initialized")
            return

        schedules = self._deps.schedules.list()
        existing_ids = {s.id for s in schedules}

        # Remove APS jobs that no longer exist in DB (but never touch internal jobs)
        for job in scheduler.get_jobs() or []:
            if job.id in _INTERNAL_JOB_IDS:
                continue
            if job.id not in existing_ids:
                self._log.info("Removing stale scheduled job %s", job.id)
                try:
                    scheduler.remove_job(job.id)
                except JobLookupError:
                    # Job removed concurrently; safe to continue
                    self._log.debug("Job %s already removed", job.id)
                except Exception:
                    self._log.exception("Failed to remove stale job %s", job.id)

        # Upsert all schedules from DB
        for sch in schedules:
            self._upsert_aps_job(sch)

    def _upsert_aps_job(self, sch: ScheduleTable) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            self._log.debug("Skipping upsert; scheduler not initialized")
            return

        try:
            trigger = self._to_trigger(sch.trigger_type, sch.trigger_args)
        except Exception:
            self._log.exception(
                "Invalid trigger for schedule %s (type=%s, args=%s)",
                sch.id,
                sch.trigger_type,
                sch.trigger_args,
            )
            return

        fn = self._job_callable(sch.id)
        try:
            scheduler.add_job(
                fn,
                trigger=trigger,
                id=sch.id,
                name=f"{sch.name}:{sch.job_id}",
                replace_existing=True,
                coalesce=True,
                max_instances=1,
            )
            if sch.is_paused:
                try:
                    scheduler.pause_job(sch.id)
                except JobLookupError:
                    self._log.warning(
                        "Job %s just created but not found to pause", sch.id
                    )
            self._log.info(
                "Scheduled '%s' (job_id=%s) with %s trigger (paused=%s)",
                sch.name,
                sch.job_id,
                sch.trigger_type,
                sch.is_paused,
            )
        except Exception:
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
        sch = await asyncio.to_thread(self._deps.schedules.get, schedule_id)
        if sch is None:
            self._log.warning("Schedule %s not found; removing from APS", schedule_id)
            try:
                scheduler = self._scheduler
                if scheduler is not None:
                    scheduler.remove_job(schedule_id)
            except JobLookupError:
                pass
            except Exception:
                self._log.exception(
                    "Failed to remove missing schedule %s from APS", schedule_id
                )
            return

        # Run in a thread to avoid blocking the loop
        try:
            runtime_job = await asyncio.to_thread(
                self._deps.jobs.load_runtime_job, sch.job_id
            )
        except Exception as exc:
            self._log.exception(
                "Failed to load job id '%s' for schedule %s: %s",
                sch.job_id,
                schedule_id,
                exc,
            )
            return

        env = sch.context
        safe_env = self._context_for_log(env)
        self._log.info(
            "Executing scheduled job id '%s' (env=%s)", sch.job_id, safe_env
        )
        try:
            await asyncio.to_thread(self._deps.executor.execute_job, runtime_job, env)
        except ExecutionAlreadyRunning:
            self._log.warning(
                "Scheduled job id '%s' already running; skipping trigger", sch.job_id
            )
        except Exception:
            self._log.exception("Scheduled job id '%s' failed", sch.job_id)
        else:
            self._log.info("Finished scheduled job id '%s'", sch.job_id)

    @staticmethod
    def _context_for_log(value: Optional[str]) -> str:
        if not value:
            return "<none>"
        if value.upper() in {"DEV", "TEST", "PROD"}:
            return value.upper()
        if re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", value):
            return value
        return "<masked>"

    def add_schedule(self, sch: ScheduleTable) -> None:
        self._upsert_aps_job(sch)

    def remove_schedule(self, schedule_id: str) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            return
        try:
            scheduler.remove_job(schedule_id)
        except JobLookupError:
            self._log.debug("Attempted to remove missing job %s", schedule_id)
        except Exception:
            self._log.exception("Failed to remove schedule %s from APS", schedule_id)

    def pause_schedule(self, schedule_id: str) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            return
        try:
            scheduler.pause_job(schedule_id)
        except JobLookupError:
            self._log.debug("Attempted to pause missing job %s", schedule_id)
        except Exception:
            self._log.exception("Failed to pause schedule %s", schedule_id)

    def resume_schedule(self, schedule_id: str) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            return
        try:
            scheduler.resume_job(schedule_id)
        except JobLookupError:
            self._log.debug("Attempted to resume missing job %s", schedule_id)
        except Exception:
            self._log.exception("Failed to resume schedule %s", schedule_id)

    async def run_now(self, schedule_id: str) -> None:
        await self._run_schedule(schedule_id)

    def _ensure_scheduler(self) -> AsyncIOScheduler:
        scheduler = self._scheduler
        if scheduler is not None:
            scheduler_loop = getattr(scheduler, "_eventloop", None)
            if (
                scheduler_loop is None
                or getattr(scheduler_loop, "is_closed", lambda: True)()
            ):
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
