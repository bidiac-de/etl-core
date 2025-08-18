from typing import Any, Dict, Annotated

from sqlalchemy.exc import SQLAlchemyError
from fastapi import APIRouter, Depends, HTTPException, status

from src.api.dependencies import get_execution_handler, get_job_handler
from src.job_execution.job_execution_handler import JobExecutionHandler
from src.persistance.errors import PersistNotFoundError
from src.persistance.handlers.job_handler import JobHandler
from src.api.helpers import _error_payload, _exc_meta

router = APIRouter(prefix="/execution", tags=["execution"])


@router.post(
    "/{job_id}",
    summary="Start a job execution",
    status_code=status.HTTP_200_OK,
)
def start_execution(
    job_id: str,
    job_handler: Annotated[JobHandler, Depends(get_job_handler)],
    execution_handler: Annotated[JobExecutionHandler, Depends(get_execution_handler)],
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
        execution = execution_handler.execute_job(runtime_job)
        return {
            "job_id": job_id,
            "status": "started",
            "execution_id": execution.id,
            "max_attempts": execution.max_attempts,
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
