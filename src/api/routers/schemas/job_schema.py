from fastapi import APIRouter
from src.job_execution.job import Job

router = APIRouter(
    prefix="/jobs",
    tags=["jobs"],
)


@router.get(
    "/schema",
    response_model=dict,
    summary="Get Job JSON schema",
    description="Returns the JSON Schema for the Job configuration payload.",
)
def get_job_schema() -> dict:
    """
    Return the JSON Schema of the Job model as understood by Pydantic
    """
    return Job.model_json_schema()
