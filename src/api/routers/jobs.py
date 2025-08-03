from fastapi import APIRouter, HTTPException
from src.job_execution.job import Job

router = APIRouter(
    prefix="/jobs",
    tags=[""],
)


@router.post(
    "/",
    response_model=str,
    summary="Create a new Job",
    description=(
        "Creates a new Job with the provided configuration. "
        "returns the ID of the created Job."
    ),
)
def create_job(job_config: dict) -> str:
    """
    Create a new Job with the provided configuration
    !!! placeholder implementation for planning purposes
    """
    job = Job(**job_config)
    job.save()
    return job.id


@router.get(
    "/jobs/{job_id}",
    response_model=dict,
    summary="Get Job by ID",
    description=("Returns the JSON representation of the Job matching the given ID, "),
)
def get_job(id: str) -> dict:
    """
    Retrieve a Job by its ID
    !!! placeholder implementation for planning purposes
    """
    job = Job.get_by_id(id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job with ID {id} not found")

    return job.model_dump()


@router.put(
    "/jobs/{job_id}",
    response_model=str,
    summary="Update Job by ID",
    description=(
        "Updates the Job matching the given ID with the provided configuration. "
        "Returns a success message if the update was successful."
    ),
)
def update_job(id: str, job_config: dict) -> str:
    """
    Update a Job by its ID with the provided configuration
    !!! placeholder implementation for planning purposes
    """
    job = Job.get_by_id(id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job with ID {id} not found")

    job.update(**job_config)
    return f"Job with ID {id} updated successfully"


@router.delete(
    "/jobs/{job_id}",
    summary="Delete Job by ID",
    description=(
        "Deletes the Job matching the given ID. "
        "Returns a success message if the deletion was successful."
    ),
)
def delete_job(id: str) -> dict:
    """
    Delete a Job by its ID
    !!! placeholder implementation for planning purposes
    """
    job = Job.get_by_id(id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job with ID {id} not found")

    job.delete()
    return {"message": f"Job with ID {id} deleted successfully"}
