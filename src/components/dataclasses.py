from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator, ConfigDict
from uuid import uuid4


class Layout(BaseModel):
    """
    Layout class to define the position of a component
    """

    model_config = ConfigDict(validate_assignment=True)

    id: str = Field(default_factory=lambda: str(uuid4()), exclude=True)
    x_coordinate: float = Field(default=0, alias="x_coord")
    y_coordinate: float = Field(default=0, alias="y_coord")

    @field_validator("id", mode="before")
    @classmethod
    def validate_id(cls, value):
        """
        Id is read-only and should not be set manually.
        """
        raise ValueError("Id is read-only and should not be set manually.")

    @field_validator("x_coordinate", "y_coordinate", mode="before")
    @classmethod
    def validate_coordinates(cls, value: float) -> float:
        """
        Validate that the coordinates are non-negative.
        """
        if value < 0:
            raise ValueError("Coordinates must be non-negative.")
        return value

    def __repr__(self):
        return (
            f"Layout(x_coordinate={self.x_coordinate}"
            f", y_coordinate={self.y_coordinate})"
        )


class MetaData(BaseModel):
    """
    Metadata class to store additional information about
    a job or a component
    """

    model_config = ConfigDict(validate_assignment=True)

    id: str = Field(default_factory=lambda: str(uuid4()), exclude=True)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: Optional[datetime] = Field(default=None, exclude=True)
    created_by: int = Field(default=0)
    updated_by: Optional[int] = Field(default=None)

    @field_validator("id", mode="before")
    @classmethod
    def validate_id(cls, value):
        """
        Id is read-only and should not be set manually
        """
        raise ValueError("Id is read-only and should not be set manually.")

    @field_validator("created_at", "updated_at", mode="before")
    @classmethod
    def validate_timestamps(cls, value: datetime) -> datetime:
        """
        Validate that datetime values are not in the future
        """
        if value > datetime.now():
            raise ValueError("Timestamp cannot be in the future.")
        return value

    @field_validator("created_by", "updated_by", mode="before")
    @classmethod
    def validate_user_ids(cls, value: int) -> int:
        """
        Validate that user IDs are non-negative integers
        """
        if not isinstance(value, int) or value < 0:
            raise ValueError("User ID must be a non-negative integer.")
        return value
