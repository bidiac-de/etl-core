from typing import Annotated, Any, Dict
from fastapi import APIRouter, Depends, HTTPException, status

from src.api.helpers import _error_payload, _exc_meta
from src.api.dependencies import get_execution_handler, get_job_handler
from src.job_execution.job_execution_handler import JobExecutionHandler
from src.persistance.handlers.job_handler import JobHandler

router = APIRouter(prefix="/execution", tags=["execution"])


@router.post(
    "/{job_id}",
    summary="Start a job execution",
    status_code=status.HTTP_200_OK,
)
def start_job_execution(
    job_id: str,
    job_handler: Annotated[JobHandler, Depends(get_job_handler)],
    execution_handler: Annotated[JobExecutionHandler, Depends(get_execution_handler)],
) -> Dict[str, Any]:
    # load job by id
    record = job_handler.get_by_id(job_id)
    if record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=_error_payload(
                "JOB_NOT_FOUND",
                f"Job {job_id!r} was not found.",
                job_id=job_id,
            ),
        )

    # rebuild runtime job
    try:
        runtime = job_handler.record_to_job(record)
    except ValueError as exc:
        # record existed in first query but not on re-fetch
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=_error_payload(
                "JOB_NOT_FOUND_ON_REFETCH",
                f"Job {job_id!r} disappeared while preparing execution.",
                job_id=job_id,
                **_exc_meta(exc),
            ),
        ) from exc

    # execute the job
    try:
        execution = execution_handler.execute_job(runtime)
        return {
            "started job with id": job_id,
            "status": "started",
            "execution_id": execution.id,
            "max_attempts": execution.max_attempts,
        }
    except Exception as exc:  # pragma: no cover - safety net
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_error_payload(
                "EXECUTION_START_FAILED",
                "Failed to start job execution.",
                job_id=job_id,
                **_exc_meta(exc),
            ),
        ) from exc
