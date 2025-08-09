from sqlmodel import SQLModel, Field
from datetime import datetime
from typing import Optional


class LayoutBase(SQLModel):
    x_coordinate: int = Field(default=0, alias="x_coord")
    y_coordinate: int = Field(default=0, alias="y_coord")


class MetaDataBase(SQLModel):
    timestamp: datetime = Field(default_factory=datetime.now)
    # optional because of CLI without user, may become default value for cli user later
    user_id: Optional[int] = Field(default=None)
