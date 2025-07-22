from src.job_execution.job_execution_handler import JobStatus
from src.metrics.base_metrics import Metrics
from datetime import datetime, timedelta

class JobMetrics(Metrics):
    """
    A class to represent job metrics.
    """

    def __init__(self,started_at: datetime, processing_time: timedelta, error_count: int = 0,
                 throughput: float = 0.0, job_status: str = JobStatus.COMPLETED.value):

        super().__init__(started_at, processing_time, error_count)
        self.throughput = throughput
        self.status = job_status


    def __repr__(self):
        return (f"JobMetrics(started_at={self.started_at}, processing_time={self.processing_time},"
                f" error_count={self.error_count}, throughput={self.throughput}, status={self.status})")