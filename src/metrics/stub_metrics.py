from datetime import datetime, timedelta
from uuid import uuid4

from pydantic import Field

from src.metrics.base_metrics import Metrics


class StubMetrics(Metrics):
    """
    Metrics for stubs, with default zeroed values.
    """

    id: str = Field(default_factory=lambda: str(uuid4()), exclude=True)
    started_at: datetime = Field(default_factory=datetime.now)
    processing_time: timedelta = Field(default_factory=lambda: timedelta(0))
    error_count: int = Field(default=0)
    lines_received: int = Field(default=0)
