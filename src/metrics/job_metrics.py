from src.metrics.base_metrics import Metrics
from datetime import datetime
from src.components.runtime_state import RuntimeState
from typing import Dict


class JobMetrics(Metrics):
    """
    A class to represent job metrics.
    """

    def __init__(self):
        super().__init__()
        self.started_at = datetime.now()
        self.status = RuntimeState.RUNNING.value

    throughput: float = 0.0

    def calc_throughput(self, total_lines: int):
        self.throughput = (
            total_lines / self.processing_time.total_seconds()
            if (self.processing_time.total_seconds() > 0)
            else 0.0
        )

    def update_metrics(self, metrics_map: Dict[str, any]) -> None:
        """
        Aggregate total lines received from component metrics,
        update processing time and throughput.
        """
        total = sum(m.lines_received for m in metrics_map.values())
        now = datetime.now()
        elapsed = now - self.started_at
        self.processing_time = elapsed
        self.throughput = (
            total / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0.0
        )

    def __repr__(self) -> str:
        return (
            f"JobMetrics(id={self.id!r}, started_at={self.started_at!r}, "
            f"processing_time={self.processing_time!r}, "
            f"error_count={self.error_count}, "
            f"throughput={self.throughput}, status={self.status!r})"
        )
