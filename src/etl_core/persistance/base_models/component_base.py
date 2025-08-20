from sqlmodel import SQLModel, Field


class ComponentBase(SQLModel):
    name: str = Field(description="Unique name of the component.")
    description: str = Field(description="A short description of the component.")
    comp_type: str = Field(description="The registered type of the component.")
