from fastapi import APIRouter, HTTPException
from src.job_execution.job import Job
from src.persistance.job_handler import JobHandler

router = APIRouter(
    prefix="/jobs",
    tags=[""],
)


# create a single handler instance to reuse across requests
job_handler = JobHandler()


@router.post(
    "/",
    response_model=str,
    summary="Create a new Job",
    description=(
        "Creates a new Job with the provided configuration "
        "and persists it to the internal database."
    ),
)
def create_job(job_config: dict) -> str:
    """
    Build a Job model from the incoming JSON, then persist via JobHandler.
    """
    job = Job(**job_config)
    try:
        job_handler.create(job)
    except Exception as e:
        # wrap any persistence errors as 500s
        raise HTTPException(status_code=500, detail=f"Failed to persist job: {e}")

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
