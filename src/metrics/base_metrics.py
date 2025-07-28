from abc import ABC
from datetime import datetime, timedelta
from uuid import uuid4

from pydantic import BaseModel, Field


class Metrics(BaseModel, ABC):
    """
    Base class for metrics.
    """

    id: str = Field(default_factory=lambda: str(uuid4()), exclude=True)
    started_at: datetime
    processing_time: timedelta
    error_count: int
