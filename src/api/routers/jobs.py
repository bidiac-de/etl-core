from typing import Annotated, Dict, List

from fastapi import APIRouter, Depends, HTTPException

from src.api.dependencies import get_job_handler
from src.persistance.configs.job_config import JobConfig
from src.persistance.handlers.job_handler import JobHandler

router = APIRouter(prefix="/jobs", tags=["Jobs"])


def _sanitize_errors(errors: List[Dict]) -> List[Dict]:
    sanitized: List[Dict] = []
    for err in errors:
        filtered: Dict = {}
        for key in ("type", "loc", "msg", "url"):
            if key in err:
                filtered[key] = err[key]
        sanitized.append(filtered)
    return sanitized


@router.post(
    "/",
    response_model=str,
    summary="Create a new Job",
    description="Creates a new Job with the provided configuration and persists it.",
)
def create_job(
    job_cfg: JobConfig,
    job_handler: Annotated[JobHandler, Depends(get_job_handler)],
) -> str:
    try:
        entry = job_handler.create_job_entry(job_cfg)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=500, detail=f"Failed to persist job: {exc}"
        ) from exc
    return entry.id


@router.get(
    "/{job_id}",
    response_model=Dict,
    summary="Get Job by ID",
    description="Returns the JSON representation of the Job matching the given ID.",
)
def get_job(
    job_id: str,
    job_handler: Annotated[JobHandler, Depends(get_job_handler)],
) -> Dict:
    record = job_handler.get_by_id(job_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found")
    job = job_handler.record_to_job(record)
    result = job.model_dump()
    result["id"] = job.id
    return result


@router.put("/{job_id}", response_model=str)
def update_job(
    job_id: str,
    job_cfg: JobConfig,
    job_handler: Annotated[JobHandler, Depends(get_job_handler)],
) -> str:
    try:
        row = job_handler.update(job_id, job_cfg)
    except ValueError as not_found:
        raise HTTPException(status_code=404, detail=str(not_found)) from not_found
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=500, detail=f"Failed to update job: {exc}"
        ) from exc
    return row.id


@router.delete(
    "/{job_id}",
    response_model=Dict,
    summary="Delete Job by ID",
    description="Deletes the Job matching the given ID.",
)
def delete_job(
    job_id: str,
    job_handler: Annotated[JobHandler, Depends(get_job_handler)],
) -> Dict:
    record = job_handler.get_by_id(job_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found")
    try:
        job_handler.delete(job_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=500, detail=f"Failed to delete job: {exc}"
        ) from exc
    return {"message": f"Job {job_id!r} deleted successfully"}


@router.get(
    "/",
    response_model=List[Dict],
    summary="List all Jobs (no components)",
    description="Returns each job’s data—excluding components but including metadata.",
)
def list_jobs(
    job_handler: Annotated[JobHandler, Depends(get_job_handler)],
) -> List[Dict]:
    records = job_handler.get_all()
    if not records:
        return []

    jobs: List[Dict] = []
    for record in records:
        runtime_obj = job_handler.record_to_job(record)
        obj_dict = runtime_obj.model_dump(exclude={"components"})
        obj_dict["id"] = record.id
        jobs.append(obj_dict)

    return jobs
