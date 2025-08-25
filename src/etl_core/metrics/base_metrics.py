from abc import ABC
from datetime import datetime, timedelta
from uuid import uuid4
from pydantic import BaseModel, PrivateAttr
from etl_core.components.runtime_state import RuntimeState


class Metrics(BaseModel, ABC):
    """
    Base class for metrics.
    """

    _id: str = PrivateAttr(default_factory=lambda: str(uuid4()))
    _status: str = RuntimeState.PENDING.value
    _created_at: datetime = datetime.now()
    _started_at: datetime
    _processing_time: timedelta
    _error_count: int = 0

    def set_started(self):
        """
        Set the started_at time and reset processing_time.
        """
        self.started_at = datetime.now()
        self.status = RuntimeState.RUNNING.value

    @property
    def id(self) -> str:
        """
        Get the unique identifier of the component
        :return: Unique identifier as a string
        """
        return self._id

    @property
    def status(self) -> str:
        return self._status

    @status.setter
    def status(self, value: str) -> None:
        if value not in RuntimeState:
            raise ValueError(f"Invalid status: {value}")
        self._status = value

    @property
    def created_at(self) -> datetime:
        return self._created_at

    @property
    def started_at(self) -> datetime:
        return self._started_at

    @started_at.setter
    def started_at(self, value: datetime) -> None:
        if not isinstance(value, datetime):
            raise ValueError("started_at must be a datetime object")
        self._started_at = value
        self.processing_time = timedelta(0)

    @property
    def processing_time(self) -> timedelta:
        return self._processing_time

    @processing_time.setter
    def processing_time(self, value: timedelta) -> None:
        if not isinstance(value, timedelta):
            raise ValueError("processing_time must be a timedelta object")
        self._processing_time = value

    @property
    def error_count(self) -> int:
        return self._error_count

    @error_count.setter
    def error_count(self, value: int) -> None:
        if not isinstance(value, int) or value < 0:
            raise ValueError("error_count must be a non-negative integer")
        self._error_count = value
