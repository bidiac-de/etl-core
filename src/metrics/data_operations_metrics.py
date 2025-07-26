from datetime import datetime, timedelta
from src.metrics.component_metrics import ComponentMetrics


class DataOperationsMetrics(ComponentMetrics):
    """
    A class that offers metrics for data processing components.
    """

    def __init__(
        self,
        started_at: datetime,
        processing_time: timedelta,
        error_count: int,
        lines_received: int = 0,
        lines_forwarded: int = 0,
        lines_processed: int = 0,
    ):
        super().__init__(
            started_at, processing_time, error_count, lines_received, lines_forwarded
        )
        self.lines_processed = lines_processed

    def __repr__(self):
        base = super().__repr__()[:-1]
        return f"{base}, lines_processed={self.lines_processed})"


class FilterMetrics(DataOperationsMetrics):
    """
    A class that offers metrics for filter operations.
    """

    def __init__(
        self,
        started_at: datetime,
        processing_time: timedelta,
        error_count: int,
        lines_received: int = 0,
        lines_forwarded: int = 0,
        lines_processed: int = 0,
        lines_dismissed: int = 0,
    ):
        super().__init__(
            started_at,
            processing_time,
            error_count,
            lines_received,
            lines_forwarded,
            lines_processed,
        )
        self.lines_dismissed = lines_dismissed

    def __repr__(self):
        base = super().__repr__()[:-1]
        return f"{base}, lines_dismissed={self.lines_dismissed})"
