from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Annotated, Optional

from sqlalchemy.exc import SQLAlchemyError
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from etl_core.api.dependencies import (
    get_execution_handler,
    get_job_handler,
    get_execution_records_handler,
)
from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from etl_core.persistance.errors import PersistNotFoundError
from etl_core.persistance.handlers.execution_records_handler import (
    ExecutionRecordsHandler,
)
from etl_core.persistance.handlers.job_handler import JobHandler
from etl_core.api.helpers import _error_payload, _exc_meta
from etl_core.context.environment import Environment

router = APIRouter(prefix="/execution", tags=["execution"])


class StartExecutionBody(BaseModel):
    environment: Environment


@router.post(
    "/{job_id}",
    summary="Start a job execution",
    status_code=status.HTTP_200_OK,
)
def start_execution(
    job_id: str,
    job_handler: Annotated[JobHandler, Depends(get_job_handler)],
    execution_handler: Annotated[JobExecutionHandler, Depends(get_execution_handler)],
    body: Optional[StartExecutionBody] = None,
) -> Dict[str, Any]:
    try:
        runtime_job = job_handler.load_runtime_job(job_id)
    except PersistNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=_error_payload("JOB_NOT_FOUND", str(exc), job_id=job_id),
        ) from exc
    except SQLAlchemyError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_error_payload("DB_ERROR", "Failed to load job.", **_exc_meta(exc)),
        ) from exc

    try:
        env = body.environment if body else None
        execution = execution_handler.execute_job(runtime_job, environment=env)
        return {
            "job_id": job_id,
            "status": "started",
            "execution_id": execution.id,
            "max_attempts": execution.max_attempts,
            "environment": env.value if isinstance(env, Environment) else None,
        }
    except Exception as exc:  # pragma: no cover
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_error_payload(
                "EXECUTION_START_FAILED",
                "Failed to start job execution.",
                job_id=job_id,
                **_exc_meta(exc),
            ),
        ) from exc


class ExecutionOut(BaseModel):
    id: str
    job_id: str
    environment: Optional[str] = None
    status: str
    error: Optional[str] = None
    started_at: datetime
    finished_at: Optional[datetime] = None


class ExecutionAttemptOut(BaseModel):
    id: str
    execution_id: str
    attempt_index: int
    status: str
    error: Optional[str] = None
    started_at: datetime
    finished_at: Optional[datetime] = None


class ExecutionListOut(BaseModel):
    data: list[ExecutionOut]


class ExecutionDetailOut(BaseModel):
    execution: ExecutionOut
    attempts: list[ExecutionAttemptOut]


def _to_exec_out(row) -> ExecutionOut:
    return ExecutionOut(
        id=row.id,
        job_id=row.job_id,
        environment=row.environment,
        status=row.status,
        error=row.error,
        started_at=row.started_at,
        finished_at=row.finished_at,
    )


def _to_attempt_out(row) -> ExecutionAttemptOut:
    return ExecutionAttemptOut(
        id=row.id,
        execution_id=row.execution_id,
        attempt_index=row.attempt_index,
        status=row.status,
        error=row.error,
        started_at=row.started_at,
        finished_at=row.finished_at,
    )


@router.get(
    "/executions",
    response_model=ExecutionListOut,
    summary="List executions with filters",
)
def list_executions(
    _records: Annotated[
        ExecutionRecordsHandler, Depends(get_execution_records_handler)
    ],
    job_id: Optional[str] = None,
    status: Optional[str] = None,
    environment: Optional[str] = None,
    started_after: Optional[datetime] = Query(default=None),
    started_before: Optional[datetime] = Query(default=None),
    sort_by: str = Query(
        default="started_at", pattern="^(started_at|finished_at|status)$"
    ),
    order: str = Query(default="desc", pattern="^(asc|desc)$"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> ExecutionListOut:
    rows, _ = _records.list_executions(
        job_id=job_id,
        status=status,
        environment=environment,
        started_after=started_after,
        started_before=started_before,
        sort_by=sort_by,
        order=order,
        limit=limit,
        offset=offset,
    )
    return ExecutionListOut(
        data=[_to_exec_out(r) for r in rows],
    )


@router.get(
    "/executions/{execution_id}",
    response_model=ExecutionDetailOut,
    summary="Get one execution (with attempts)",
)
def get_execution(
    execution_id: str,
    _records: Annotated[
        ExecutionRecordsHandler, Depends(get_execution_records_handler)
    ],
) -> ExecutionDetailOut:
    row, attempts = _records.get_execution(execution_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Execution not found")
    return ExecutionDetailOut(
        execution=_to_exec_out(row),
        attempts=[_to_attempt_out(a) for a in attempts],
    )


@router.get(
    "/executions/{execution_id}/attempts",
    response_model=list[ExecutionAttemptOut],
    summary="List attempts for one execution",
)
def list_attempts(
    execution_id: str,
    _records: Annotated[
        ExecutionRecordsHandler, Depends(get_execution_records_handler)
    ],
) -> list[ExecutionAttemptOut]:
    exec_row, _ = _records.get_execution(execution_id)
    if exec_row is None:
        raise HTTPException(status_code=404, detail="Execution not found")
    rows = _records.list_attempts(execution_id)
    return [_to_attempt_out(r) for r in rows]
