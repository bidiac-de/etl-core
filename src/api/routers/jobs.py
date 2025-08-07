from fastapi import APIRouter, HTTPException
from pydantic import ValidationError
from typing import List, Dict

from src.job_execution.job import Job
from src.persistance.job_handler import JobHandler

router = APIRouter(
    prefix="/jobs",
    tags=["Jobs"],
)

job_handler = JobHandler()


def _sanitize_errors(errors: List[Dict]) -> List[Dict]:
    # Remove non-serializable fields from Pydantic errors
    sanitized: List[Dict] = []
    for err in errors:
        # Keep only essential serializable keys
        filtered = {}
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
def create_job(job_config: dict) -> str:
    try:
        job = Job(**job_config)
    except ValidationError as ve:
        clean = _sanitize_errors(ve.errors())
        raise HTTPException(status_code=422, detail=clean)
    except Exception as ve:
        raise HTTPException(status_code=500, detail=f"Failed to create job: {ve}")
    try:
        job_handler.create(job)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to persist job: {e}")
    return job.id


@router.get(
    "/{job_id}",
    response_model=Dict,
    summary="Get Job by ID",
    description="Returns the JSON representation of the Job matching the given ID.",
)
def get_job(job_id: str) -> Dict:
    record = job_handler.get_by_id(job_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found")
    job = job_handler.record_to_job(record)
    result = job.model_dump()
    result["id"] = job.id
    return result


@router.put(
    "/{job_id}",
    response_model=str,
    summary="Update Job by ID",
    description="Updates the Job matching the given ID with the"
    " provided configuration.",
)
def update_job(job_id: str, job_config: dict) -> str:
    try:
        job = Job(**job_config)
    except ValidationError as ve:
        clean = _sanitize_errors(ve.errors())
        raise HTTPException(status_code=422, detail=clean)
    except Exception as ve:
        raise HTTPException(status_code=500, detail=f"Failed to update job: {ve}")
    object.__setattr__(job, "_id", job_id)
    try:
        job_handler.update(job)
    except ValueError as not_found:
        raise HTTPException(status_code=404, detail=str(not_found))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update job: {e}")
    return job.id


@router.delete(
    "/{job_id}",
    response_model=Dict,
    summary="Delete Job by ID",
    description="Deletes the Job matching the given ID.",
)
def delete_job(job_id: str) -> Dict:
    record = job_handler.get_by_id(job_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found")
    try:
        job_handler.delete(job_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete job: {e}")
    return {"message": f"Job {job_id!r} deleted successfully"}


@router.get(
    "/",
    response_model=List[Dict],
    summary="List all Jobs (no components)",
    description="Returns each job’s data—excluding components but including metadata.",
)
def list_jobs() -> List[Dict]:
    records = job_handler.get_all()
    if not records:
        return []

    jobs: List[Dict] = []
    for record in records:
        rec_dict = record.dict(exclude={"components"})
        rec_dict["metadata"] = rec_dict.pop("metadata_", {})
        jobs.append(rec_dict)

    return jobs
