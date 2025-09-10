from typing import Any, Dict, Annotated, Optional

from sqlalchemy.exc import SQLAlchemyError
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from etl_core.api.dependencies import get_execution_handler, get_job_handler
from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from etl_core.persistance.errors import PersistNotFoundError
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
