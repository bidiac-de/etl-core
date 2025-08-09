from sqlmodel import SQLModel


class ComponentBase(SQLModel):
    name: str
    description: str
    comp_type: str
