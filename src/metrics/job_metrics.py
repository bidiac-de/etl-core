from uuid import uuid4

from pydantic import Field

from src.job_execution.job_status import JobStatus
from src.metrics.base_metrics import Metrics


class JobMetrics(Metrics):
    """
    A class to represent job metrics.
    """

    id: str = Field(default_factory=lambda: str(uuid4()), exclude=True)
    throughput: float = 0.0
    status: str = Field(default=JobStatus.COMPLETED.value)

    def __repr__(self) -> str:
        return (
            f"JobMetrics(id={self.id!r}, started_at={self.started_at!r}, "
            f"processing_time={self.processing_time!r}, "
            f"error_count={self.error_count}, "
            f"throughput={self.throughput}, status={self.status!r})"
        )
