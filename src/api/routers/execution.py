from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from src.api.dependencies import get_execution_handler, get_job_handler
from src.job_execution.job_execution_handler import JobExecutionHandler
from src.persistance.handlers.job_handler import JobHandler

router = APIRouter(prefix="/execution", tags=["execution"])


@router.post("/{job_id}", summary="Start a job execution")
def start_job_execution(
    job_id: str,
    job_handler: Annotated[JobHandler, Depends(get_job_handler)],
    execution_handler: Annotated[JobExecutionHandler, Depends(get_execution_handler)],
) -> dict:
    record = job_handler.get_by_id(job_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found")

    try:
        runtime = job_handler.record_to_job(record)
    except ValueError as exc:
        # record existed but re-fetch by id failed (deleted concurrently?)
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    try:
        execution_handler.execute_job(runtime)
        return {"started job with id": job_id, "status": "started"}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=500, detail=f"Failed to execute job: {exc}"
        ) from exc
