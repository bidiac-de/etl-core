from .base import Metrics
from datetime import time, datetime


class ComponentMetrics(Metrics):
    """Base class for metrics related to components."""

    def __init__(self, started_at: datetime, processing_time: time, error_count: int,
                 lines_received: int = 0, lines_forwarded: int = 0):
        super().__init__(started_at, processing_time, error_count)
        self.lines_received = lines_received
        self.lines_forwarded = lines_forwarded


class DatabaseMetrics(ComponentMetrics):
    """Metrics for database operations."""

    def __init__(self, started_at: datetime, processing_time: time, error_count: int,
                 lines_received: int = 0, lines_forwarded: int = 0,
                 query_execution_time: float = 0.0):
        super().__init__(started_at, processing_time, error_count, lines_received, lines_forwarded)
        self.query_execution_time = query_execution_time


class ReadMetrics(DatabaseMetrics):
    """Metrics for reading operations."""

    def __init__(self, started_at: datetime, processing_time: time, error_count: int,
                 lines_received: int = 0, lines_forwarded: int = 0,
                 query_execution_time: float = 0.0, lines_read: int = 0):
        super().__init__(started_at, processing_time, error_count,
                         lines_received, lines_forwarded, query_execution_time)
        self.lines_read = lines_read


class WriteMetrics(DatabaseMetrics):
    """Metrics for writing operations."""

    def __init__(self, started_at: datetime, processing_time: time, error_count: int,
                 lines_received: int = 0, lines_forwarded: int = 0,
                 query_execution_time: float = 0.0, lines_written: int = 0):
        super().__init__(started_at, processing_time, error_count,
                         lines_received, lines_forwarded, query_execution_time)
        self.lines_written = lines_written


class ProcessMetrics(ComponentMetrics):
    """Metrics for data processing components."""

    def __init__(self, started_at: datetime, processing_time: time, error_count: int,
                 lines_received: int = 0, lines_forwarded: int = 0,
                 lines_processed: int = 0):
        super().__init__(started_at, processing_time, error_count, lines_received, lines_forwarded)
        self.lines_processed = lines_processed


class FilterMetrics(ProcessMetrics):
    """Metrics for filter operations."""

    def __init__(self, started_at: datetime, processing_time: time, error_count: int,
                 lines_received: int = 0, lines_forwarded: int = 0,
                 lines_processed: int = 0, lines_dismissed: int = 0):
        super().__init__(started_at, processing_time, error_count,
                         lines_received, lines_forwarded, lines_processed)
        self.lines_dismissed = lines_dismissed