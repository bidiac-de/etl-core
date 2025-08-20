from datetime import datetime, timedelta
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics


class DatabaseMetrics(ComponentMetrics):
    """
    A class to offer metrics for database operations.
    """

    def __init__(
        self,
        started_at: datetime,
        processing_time: timedelta,
        error_count: int,
        lines_received: int = 0,
        lines_forwarded: int = 0,
        query_execution_time: float = 0.0,
    ):
        super().__init__(
            started_at, processing_time, error_count, lines_received, lines_forwarded
        )
        self.query_execution_time = query_execution_time

    def __repr__(self):
        base = super().__repr__()[:-1]
        return f"{base}, query_execution_time={self.query_execution_time})"


class ReadMetrics(DatabaseMetrics):
    """
    A class that offers metrics for reading operations.
    """

    def __init__(
        self,
        started_at: datetime,
        processing_time: timedelta,
        error_count: int,
        lines_received: int = 0,
        lines_forwarded: int = 0,
        query_execution_time: float = 0.0,
        lines_read: int = 0,
    ):
        super().__init__(
            started_at,
            processing_time,
            error_count,
            lines_received,
            lines_forwarded,
            query_execution_time,
        )
        self.lines_read = lines_read

    def __repr__(self):
        base = super().__repr__()[:-1]
        return f"{base}, lines_read={self.lines_read})"


class WriteMetrics(DatabaseMetrics):
    """
    A class that offers metrics for writing operations.
    """

    def __init__(
        self,
        started_at: datetime,
        processing_time: timedelta,
        error_count: int,
        lines_received: int = 0,
        lines_forwarded: int = 0,
        query_execution_time: float = 0.0,
        lines_written: int = 0,
    ):
        super().__init__(
            started_at,
            processing_time,
            error_count,
            lines_received,
            lines_forwarded,
            query_execution_time,
        )
        self.lines_written = lines_written

    def __repr__(self):
        base = super().__repr__()[:-1]
        return f"{base}, lines_written={self.lines_written})"
