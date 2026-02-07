from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from etl_core.api.http_errors import http_404, http_500_exc
from etl_core.persistence.handlers.schedule_handler import ScheduleNotFoundError
from etl_core.persistence.table_definitions import ScheduleTable, TriggerType
from etl_core.scheduling.commands import (
    CreateScheduleCommand,
    ListSchedulesCommand,
    GetScheduleCommand,
    UpdateScheduleCommand,
    DeleteScheduleCommand,
    PauseScheduleCommand,
    ResumeScheduleCommand,
    RunNowScheduleCommand,
)
from etl_core.singletons import schedule_handler as _schedule_handler_singleton


router = APIRouter(prefix="/schedules", tags=["schedules"])


class ScheduleIn(BaseModel):
    name: str
    job_id: str
    environment: str = Field(description="Execution environment gate: DEV/TEST/PROD")
    trigger_type: TriggerType
    trigger_args: Dict[str, Any] = Field(default_factory=dict)
    paused: bool = False


class SchedulePatch(BaseModel):
    name: Optional[str] = None
    job_id: Optional[str] = None
    environment: Optional[str] = None
    trigger_type: Optional[TriggerType] = None
    trigger_args: Optional[Dict[str, Any]] = None
    paused: Optional[bool] = None


class ScheduleOut(BaseModel):
    id: str
    name: str
    job_id: str
    environment: str
    trigger_type: TriggerType
    trigger_args: Dict[str, Any]
    is_paused: bool

    @classmethod
    def from_row(cls, row: ScheduleTable) -> "ScheduleOut":
        return cls(
            id=row.id,
            name=row.name,
            job_id=row.job_id,
            environment=row.environment,
            trigger_type=row.trigger_type,
            trigger_args=row.trigger_args,
            is_paused=row.is_paused,
        )


@router.post("/", response_model=str, status_code=status.HTTP_201_CREATED)
def create_schedule(body: ScheduleIn) -> str:
    try:
        cmd = CreateScheduleCommand(
            name=body.name,
            job_id=body.job_id,
            environment=body.environment,
            trigger_type=body.trigger_type,
            trigger_args=body.trigger_args,
            paused=body.paused,
        )
        row = cmd.execute()
        return row.id
    except Exception as exc:  # noqa: BLE001
        raise http_500_exc(
            "SCHEDULE_CREATE_FAILED",
            "Failed to create schedule.",
            exc,
        ) from exc


@router.get("/", response_model=List[ScheduleOut])
def list_schedules() -> List[ScheduleOut]:
    try:
        rows = ListSchedulesCommand(schedules=_schedule_handler_singleton()).execute()
        return [ScheduleOut.from_row(r) for r in rows]
    except Exception as exc:  # noqa: BLE001
        raise http_500_exc(
            "SCHEDULE_LIST_FAILED",
            "Failed to list schedules.",
            exc,
        ) from exc


@router.get("/{schedule_id}", response_model=ScheduleOut)
def get_schedule(schedule_id: str) -> ScheduleOut:
    try:
        row = GetScheduleCommand(
            schedule_id=schedule_id, schedules=_schedule_handler_singleton()
        ).execute()
        return ScheduleOut.from_row(row)
    except ScheduleNotFoundError as exc:
        raise http_404(
            "SCHEDULE_NOT_FOUND",
            "Schedule not found.",
            schedule_id=schedule_id,
        ) from exc
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise http_500_exc(
            "SCHEDULE_GET_FAILED",
            "Failed to load schedule.",
            exc,
            schedule_id=schedule_id,
        ) from exc


@router.put("/{schedule_id}", response_model=str)
def update_schedule(schedule_id: str, patch: SchedulePatch) -> str:
    try:
        row = UpdateScheduleCommand(
            schedule_id=schedule_id,
            name=patch.name,
            job_id=patch.job_id,
            environment=patch.environment,
            trigger_type=patch.trigger_type,
            trigger_args=patch.trigger_args,
            paused=patch.paused,
        ).execute()
        return row.id
    except ScheduleNotFoundError as exc:
        raise http_404(
            "SCHEDULE_NOT_FOUND",
            "Schedule not found.",
            schedule_id=schedule_id,
        ) from exc
    except Exception as exc:  # noqa: BLE001
        raise http_500_exc(
            "SCHEDULE_UPDATE_FAILED",
            "Failed to update schedule.",
            exc,
            schedule_id=schedule_id,
        ) from exc


@router.delete("/{schedule_id}", response_model=dict)
def delete_schedule(schedule_id: str) -> Dict[str, str]:
    try:
        DeleteScheduleCommand(schedule_id).execute()
        return {"message": f"Schedule {schedule_id} deleted"}
    except ScheduleNotFoundError as exc:
        raise http_404(
            "SCHEDULE_NOT_FOUND",
            "Schedule not found.",
            schedule_id=schedule_id,
        ) from exc
    except Exception as exc:  # noqa: BLE001
        raise http_500_exc(
            "SCHEDULE_DELETE_FAILED",
            "Failed to delete schedule.",
            exc,
            schedule_id=schedule_id,
        ) from exc


@router.post("/{schedule_id}/pause", response_model=ScheduleOut)
def pause_schedule(schedule_id: str) -> ScheduleOut:
    try:
        row = PauseScheduleCommand(schedule_id).execute()
        return ScheduleOut.from_row(row)
    except ScheduleNotFoundError as exc:
        raise http_404(
            "SCHEDULE_NOT_FOUND",
            "Schedule not found.",
            schedule_id=schedule_id,
        ) from exc
    except Exception as exc:  # noqa: BLE001
        raise http_500_exc(
            "SCHEDULE_PAUSE_FAILED",
            "Failed to pause schedule.",
            exc,
            schedule_id=schedule_id,
        ) from exc


@router.post("/{schedule_id}/resume", response_model=ScheduleOut)
def resume_schedule(schedule_id: str) -> ScheduleOut:
    try:
        row = ResumeScheduleCommand(schedule_id).execute()
        return ScheduleOut.from_row(row)
    except ScheduleNotFoundError as exc:
        raise http_404(
            "SCHEDULE_NOT_FOUND",
            "Schedule not found.",
            schedule_id=schedule_id,
        ) from exc
    except Exception as exc:  # noqa: BLE001
        raise http_500_exc(
            "SCHEDULE_RESUME_FAILED",
            "Failed to resume schedule.",
            exc,
            schedule_id=schedule_id,
        ) from exc


@router.post("/{schedule_id}/run-now", response_model=dict)
async def run_now(schedule_id: str) -> Dict[str, str]:
    try:
        await RunNowScheduleCommand(schedule_id).execute()
        return {"status": "started"}
    except ScheduleNotFoundError as exc:
        raise http_404(
            "SCHEDULE_NOT_FOUND",
            "Schedule not found.",
            schedule_id=schedule_id,
        ) from exc
    except Exception as exc:  # noqa: BLE001
        raise http_500_exc(
            "SCHEDULE_RUN_NOW_FAILED",
            "Failed to start schedule.",
            exc,
            schedule_id=schedule_id,
        ) from exc
