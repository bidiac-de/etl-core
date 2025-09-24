from sqlmodel import SQLModel
from pydantic import Field


class ComponentBase(SQLModel):
    name: str = Field(description="Unique name of the component.")
    description: str = Field(description="A short description of the component.")
    comp_type: str = Field(
        description="The registered type of the component.",
        json_schema_extra={"used_in_table": False},
    )
