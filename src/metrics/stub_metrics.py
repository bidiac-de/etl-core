from datetime import datetime, timedelta
from src.metrics.base_metrics import Metrics


class StubMetrics(Metrics):
    lines_received: int

    def __init__(self, lines_received: int = 0):
        super().__init__(
            started_at=datetime.now(), processing_time=timedelta(0), error_count=0
        )
        self.lines_received = lines_received
