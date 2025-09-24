from datetime import datetime, timezone
from pydantic import field_validator, ConfigDict, PrivateAttr
from uuid import uuid4
from typing import Optional

from etl_core.persistence.base_models.dataclasses_base import LayoutBase, MetaDataBase


class Layout(LayoutBase):
    """
    Layout class to define the position of a component
    """

    model_config = ConfigDict(validate_assignment=True)

    _id: str = PrivateAttr(default_factory=lambda: str(uuid4()))

    @field_validator("x_coordinate", "y_coordinate", mode="before")
    @classmethod
    def validate_coordinates(cls, value: int) -> int:
        """
        Validate that the coordinates are integer
        """
        if not isinstance(value, int):
            raise ValueError("Coordinates must be integer.")
        return value

    @property
    def id(self) -> str:
        """
        Get the unique identifier of the component
        :return: Unique identifier as a string
        """
        return self._id

    def __repr__(self) -> str:
        return (
            f"Layout(x_coordinate={self.x_coordinate}"
            f", y_coordinate={self.y_coordinate})"
        )


class MetaData(MetaDataBase):
    """
    Metadata class to store additional information about
    a job or a component
    """

    model_config = ConfigDict(validate_assignment=True)

    _id: str = PrivateAttr(default_factory=lambda: str(uuid4()))

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamps(cls, value: datetime) -> datetime:
        """
        Accept ISO strings or datetime; interpret naive datetimes as local time,
        then compare in UTC with small skew tolerance.
        """
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value)
            except ValueError as exc:
                raise ValueError(
                    "Timestamp must be ISO-format datetime string or datetime object."
                ) from exc

        if not isinstance(value, datetime):
            raise ValueError("Timestamp must be a datetime.")

        # If naive, assume local timezone
        if value.tzinfo is None:
            local_tz = datetime.now().astimezone().tzinfo
            value_local = value.replace(tzinfo=local_tz)
        else:
            value_local = value

        # Compare in UTC
        candidate_utc = value_local.astimezone(timezone.utc)
        now_utc = datetime.now(timezone.utc)
        if candidate_utc > now_utc:
            raise ValueError("Timestamp cannot be in the future.")
        return value

    @field_validator("user_id", mode="before")
    @classmethod
    def validate_user_ids(cls, value: Optional[int]):
        """
        Validate that set user IDs are non-negative integers
        """
        if value is None:
            return None
        if not isinstance(value, int) or value < 0:
            raise ValueError("User ID must be a non-negative integer.")
        return value

    @property
    def id(self) -> str:
        """
        Get the unique identifier of the component
        :return: Unique identifier as a string
        """
        return self._id
