from fastapi import APIRouter

router = APIRouter(
    prefix="/ws/jobs/metrics",
    tags=["metrics"],
)


@router.get(
    "/{job_id}",
    summary="Get job metrics",
    description="Returns live metrics for the specified job via websocket connection",
)
def get_job_metrics(job_id: str):
    """
    Get live metrics for the specified job.
    This is a placeholder implementation for planning purposes.
    """
    # Placeholder logic to return job metrics
    return {"job_id": job_id, "metrics": "Live metrics data will be here."}
