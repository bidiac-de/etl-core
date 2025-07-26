from datetime import datetime

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


class MetaData:
    """
    Metadata class to store additional information about a job or a component
    """

    def __init__(self, created_at: datetime, created_by: int):
        self.created_at = created_at
        self.updated_at = None
        self.created_by = created_by
        self.updated_by = None

    def set_end_time(self, updated_at: datetime):
        self.updated_at = updated_at

    def set_status(self, updated_by: int):
        self.updated_by = updated_by
