from abc import ABC
from datetime import datetime, timedelta
from uuid import uuid4
from pydantic import BaseModel, Field
from src.components.runtime_state import RuntimeState


class Metrics(BaseModel, ABC):
    """
    Base class for metrics.
    """

    id: str = Field(default_factory=lambda: str(uuid4()), exclude=True)
    status: str = Field(default=RuntimeState.PENDING.value)
    created_at: datetime = Field(default_factory=datetime.now)
    started_at: datetime = None
    processing_time: timedelta = None
    error_count: int = 0

    def set_started(self):
        """
        Set the started_at time and reset processing_time.
        """
        self.started_at = datetime.now()
        self.status = RuntimeState.RUNNING.value
