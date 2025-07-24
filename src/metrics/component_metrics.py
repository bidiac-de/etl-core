from .base import Metrics
from datetime import datetime, timedelta


class ComponentMetrics(Metrics):
    """
    A class to represent job metrics.
    """

    def __init__(
        self,
        started_at: datetime,
        processing_time: timedelta,
        error_count: int = 0,
        lines_received: int = 0,
        lines_forwarded: int = 0,
    ):
        super().__init__(started_at, processing_time, error_count)
        self.lines_received = lines_received
        self.lines_forwarded = lines_forwarded

    def __repr__(self):
        base = super().__repr__()[:-1]
        return (
            f"{base}, lines_received={self.lines_received}, "
            f"lines_forwarded={self.lines_forwarded})"
        )
