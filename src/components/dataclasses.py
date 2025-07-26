from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class Layout(BaseModel):
    """
    Layout class to define the position of a component
    """

    x_coordinate: float = Field(..., alias="x_coord")
    y_coordinate: float = Field(..., alias="y_coord")

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

    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: Optional[datetime] = None
    created_by: int = 0
    updated_by: Optional[int] = None

    def set_end_time(self, updated_at: datetime) -> None:
        self.updated_at = updated_at

    def set_status(self, updated_by: int) -> None:
        self.updated_by = updated_by
