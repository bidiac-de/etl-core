from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from etl_core.persistence.handlers.schedule_handler import ScheduleHandler
from etl_core.persistence.table_definitions import TriggerType, ScheduleTable
from etl_core.scheduling.scheduler_service import SchedulerService
from etl_core.singletons import schedule_handler as _schedule_handler_singleton


class Command:
    def execute(self) -> Any:  # pragma: no cover - interface
        raise NotImplementedError


@dataclass
class CreateScheduleCommand(Command):
    name: str
    job_id: str
    environment: str
    trigger_type: TriggerType
    trigger_args: Dict[str, Any]
    paused: bool = False
    schedules: ScheduleHandler = field(default_factory=_schedule_handler_singleton)
    scheduler: SchedulerService = field(default_factory=SchedulerService.instance)

    def execute(self) -> ScheduleTable:
        row = self.schedules.create(
            name=self.name,
            job_id=self.job_id,
            environment=self.environment,
            trigger_type=self.trigger_type,
            trigger_args=self.trigger_args,
            is_paused=self.paused,
        )
        self.scheduler.add_schedule(row)
        return row


@dataclass
class UpdateScheduleCommand(Command):
    schedule_id: str
    name: Optional[str] = None
    job_id: Optional[str] = None
    environment: Optional[str] = None
    trigger_type: Optional[TriggerType] = None
    trigger_args: Optional[Dict[str, Any]] = None
    paused: Optional[bool] = None
    schedules: ScheduleHandler = field(default_factory=_schedule_handler_singleton)
    scheduler: SchedulerService = field(default_factory=SchedulerService.instance)

    def execute(self) -> ScheduleTable:
        row = self.schedules.update(
            self.schedule_id,
            name=self.name,
            job_id=self.job_id,
            environment=self.environment,
            trigger_type=self.trigger_type,
            trigger_args=self.trigger_args,
            is_paused=self.paused,
        )
        self.scheduler.add_schedule(row)
        return row


@dataclass
class DeleteScheduleCommand(Command):
    schedule_id: str
    schedules: ScheduleHandler = field(default_factory=_schedule_handler_singleton)
    scheduler: SchedulerService = field(default_factory=SchedulerService.instance)

    def execute(self) -> None:
        self.schedules.delete(self.schedule_id)
        self.scheduler.remove_schedule(self.schedule_id)


@dataclass
class PauseScheduleCommand(Command):
    schedule_id: str
    schedules: ScheduleHandler = field(default_factory=_schedule_handler_singleton)
    scheduler: SchedulerService = field(default_factory=SchedulerService.instance)

    def execute(self) -> ScheduleTable:
        row = self.schedules.set_paused(self.schedule_id, True)
        self.scheduler.pause_schedule(self.schedule_id)
        return row


@dataclass
class ResumeScheduleCommand(Command):
    schedule_id: str
    schedules: ScheduleHandler = field(default_factory=_schedule_handler_singleton)
    scheduler: SchedulerService = field(default_factory=SchedulerService.instance)

    def execute(self) -> ScheduleTable:
        row = self.schedules.set_paused(self.schedule_id, False)
        self.scheduler.resume_schedule(self.schedule_id)
        return row


@dataclass
class RunNowScheduleCommand(Command):
    schedule_id: str
    scheduler: SchedulerService = field(default_factory=SchedulerService.instance)

    async def execute(self) -> None:  # type: ignore[override]
        await self.scheduler.run_now(self.schedule_id)
