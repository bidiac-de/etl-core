from sqlmodel import SQLModel, Field
from datetime import datetime
from typing import Optional


class LayoutBase(SQLModel):
    x_coordinate: int = Field(
        default=0,
        alias="x_coord",
        description="Horizontal position of the component in the UI layout.",
    )
    y_coordinate: int = Field(
        default=0,
        alias="y_coord",
        description="Vertical position of the component in the UI layout.",
    )


class MetaDataBase(SQLModel):
    timestamp: datetime = Field(
        default_factory=datetime.now,
        description="Timestamp when the object was created.",
    )
    user_id: Optional[int] = Field(
        default=None, description="ID of the user who created the object."
    )
