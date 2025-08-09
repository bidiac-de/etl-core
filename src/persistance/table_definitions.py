from typing import List
from uuid import uuid4
from sqlalchemy.orm import foreign

from sqlmodel import Field, SQLModel, Relationship
from src.persistance.base_models.job_base import JobBase
from src.persistance.base_models.component_base import ComponentBase
from src.persistance.base_models.dataclasses_base import LayoutBase, MetaDataBase


class MetaDataTable(MetaDataBase, table=True):
    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)
    # backrefs: one metadata row can belong to one Job or one Component
    job: "JobTable" = Relationship(
        back_populates="metadata_", sa_relationship_kwargs={"uselist": False}
    )
    components: List["ComponentTable"] = Relationship(back_populates="metadata_")


class LayoutTable(LayoutBase, table=True):
    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)
    component: "ComponentTable" = Relationship(
        back_populates="layout", sa_relationship_kwargs={"uselist": False}
    )


class JobTable(JobBase, table=True):
    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)
    metadata_id: str = Field(foreign_key="metadatatable.id", nullable=False)
    metadata_: MetaDataTable = Relationship(
        back_populates="job", sa_relationship_kwargs={"uselist": False}
    )
    components: List["ComponentTable"] = Relationship(
        back_populates="job", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )


class ComponentNextLink(SQLModel, table=True):
    component_id: str = Field(foreign_key="componenttable.id", primary_key=True)
    next_id: str = Field(foreign_key="componenttable.id", primary_key=True)


class ComponentTable(ComponentBase, table=True):
    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)
    job_id: str = Field(foreign_key="jobtable.id", nullable=False)
    job: JobTable = Relationship(back_populates="components")

    layout_id: str = Field(foreign_key="layouttable.id", nullable=False)
    layout: LayoutTable = Relationship(back_populates="component")

    metadata_id: str = Field(foreign_key="metadatatable.id", nullable=False)
    metadata_: MetaDataTable = Relationship(back_populates="components")

    # self-referential many-to-many for “next”
    next_components: List["ComponentTable"] = Relationship(
        back_populates="prev_components",
        link_model=ComponentNextLink,
        sa_relationship_kwargs={
            "primaryjoin": lambda: ComponentTable.id
            == foreign(ComponentNextLink.component_id),
            "secondaryjoin": lambda: ComponentTable.id
            == foreign(ComponentNextLink.next_id),
            "foreign_keys": lambda: [
                ComponentNextLink.component_id,
                ComponentNextLink.next_id,
            ],
        },
    )
    prev_components: List["ComponentTable"] = Relationship(
        back_populates="next_components",
        link_model=ComponentNextLink,
        sa_relationship_kwargs={
            "primaryjoin": lambda: ComponentTable.id
            == foreign(ComponentNextLink.next_id),
            "secondaryjoin": lambda: ComponentTable.id
            == foreign(ComponentNextLink.component_id),
            "foreign_keys": lambda: [
                ComponentNextLink.component_id,
                ComponentNextLink.next_id,
            ],
        },
    )
