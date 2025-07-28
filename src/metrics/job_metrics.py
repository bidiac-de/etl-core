from pydantic import Field

from src.job_execution.job_status import JobStatus
from src.metrics.base_metrics import Metrics


class JobMetrics(Metrics):
    """
    A class to represent job metrics.
    """

    throughput: float = 0.0
    status: str = Field(default=JobStatus.COMPLETED.value)

    def calc_throughput(self, total_lines: int):
        self.throughput = (
            total_lines / self.processing_time.total_seconds()
            if (self.processing_time.total_seconds() > 0)
            else 0.0
        )

    def __repr__(self) -> str:
        return (
            f"JobMetrics(id={self.id!r}, started_at={self.started_at!r}, "
            f"processing_time={self.processing_time!r}, "
            f"error_count={self.error_count}, "
            f"throughput={self.throughput}, status={self.status!r})"
        )
