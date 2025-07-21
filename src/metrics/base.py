from abc import ABC
from datetime import time,datetime

class Metrics(ABC):
    """Base class for metrics."""

    def __init__(self, started_at: datetime, processing_time: time, error_count: int):
        self.started_at = started_at
        self.processing_time = processing_time
