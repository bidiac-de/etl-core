from datetime import datetime, timedelta
from src.metrics.component_metrics.component_metrics import ComponentMetrics


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
        self._lines_processed = lines_processed

    @property
    def lines_processed(self) -> int:
        return self._lines_processed

    @lines_processed.setter
    def lines_processed(self, value: int):
        if value < 0:
            raise ValueError("lines_processed cannot be negative")
        self._lines_processed = value

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
        self._lines_dismissed = lines_dismissed

    @property
    def lines_dismissed(self) -> int:
        return self._lines_dismissed

    @lines_dismissed.setter
    def lines_dismissed(self, value: int):
        if value < 0:
            raise ValueError("lines_dismissed cannot be negative")
        self._lines_dismissed = value

    def __repr__(self):
        base = super().__repr__()[:-1]
        return f"{base}, lines_dismissed={self.lines_dismissed})"
