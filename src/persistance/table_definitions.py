# NOTE: intentionally no `from __future__ import annotations` here

from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import Column, ForeignKey
from sqlalchemy.types import JSON
from sqlmodel import Field, Relationship, SQLModel

from src.persistance.base_models.component_base import ComponentBase
from src.persistance.base_models.dataclasses_base import LayoutBase, MetaDataBase
from src.persistance.base_models.job_base import JobBase

_FOREIGN_KEY_COMPONENT_TABLE = "componenttable"
_CASCADE_ALL = "all, delete-orphan"


class ComponentNextLink(SQLModel, table=True):
    component_id: str = Field(
        sa_column=Column(
            ForeignKey(_FOREIGN_KEY_COMPONENT_TABLE, ondelete="CASCADE"),
            primary_key=True,
            nullable=False,
        )
    )
    next_id: str = Field(
        sa_column=Column(
            ForeignKey(_FOREIGN_KEY_COMPONENT_TABLE, ondelete="CASCADE"),
            primary_key=True,
            nullable=False,
        )
    )


class JobTable(JobBase, table=True):
    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)

    components: list["ComponentTable"] = Relationship(
        back_populates="job",
        sa_relationship_kwargs={
            "cascade": _CASCADE_ALL,
            "passive_deletes": True,
            "single_parent": True,
        },
    )

    metadata_: Optional["MetaDataTable"] = Relationship(
        back_populates="job",
        sa_relationship_kwargs={
            "cascade": _CASCADE_ALL,
            "passive_deletes": True,
            "single_parent": True,
            "uselist": False,
        },
    )


class ComponentTable(ComponentBase, table=True):
    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)

    job_id: str = Field(
        sa_column=Column(
            ForeignKey("jobtable.id", ondelete="CASCADE"),
            nullable=False,
        )
    )
    job: "JobTable" = Relationship(
        back_populates="components",
        sa_relationship_kwargs={"passive_deletes": True},
    )

    payload: dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False),
    )

    layout: Optional["LayoutTable"] = Relationship(
        back_populates="component",
        sa_relationship_kwargs={
            "cascade": _CASCADE_ALL,
            "passive_deletes": True,
            "single_parent": True,
            "uselist": False,
        },
    )
    metadata_: Optional["MetaDataTable"] = Relationship(
        back_populates="component",
        sa_relationship_kwargs={
            "cascade": _CASCADE_ALL,
            "passive_deletes": True,
            "single_parent": True,
            "uselist": False,
        },
    )

    next_components: list["ComponentTable"] = Relationship(
        back_populates="prev_components",
        link_model=ComponentNextLink,
        sa_relationship_kwargs={
            "primaryjoin": "ComponentTable.id==ComponentNextLink.component_id",
            "secondaryjoin": "ComponentTable.id==ComponentNextLink.next_id",
            "passive_deletes": True,
        },
    )
    prev_components: list["ComponentTable"] = Relationship(
        back_populates="next_components",
        link_model=ComponentNextLink,
        sa_relationship_kwargs={
            "primaryjoin": "ComponentTable.id==ComponentNextLink.next_id",
            "secondaryjoin": "ComponentTable.id==ComponentNextLink.component_id",
            "passive_deletes": True,
        },
    )


class MetaDataTable(MetaDataBase, table=True):
    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)

    # Exactly one of these should be set, enforced by the handler
    job_id: Optional[str] = Field(
        default=None,
        sa_column=Column(
            ForeignKey("jobtable.id", ondelete="CASCADE"),
            unique=True,
            nullable=True,
        ),
    )
    component_id: Optional[str] = Field(
        default=None,
        sa_column=Column(
            ForeignKey("componenttable.id", ondelete="CASCADE"),
            unique=True,
            nullable=True,
        ),
    )

    job: Optional["JobTable"] = Relationship(back_populates="metadata_")
    component: Optional["ComponentTable"] = Relationship(back_populates="metadata_")


class LayoutTable(LayoutBase, table=True):
    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)

    component_id: str = Field(
        sa_column=Column(
            ForeignKey("componenttable.id", ondelete="CASCADE"),
            unique=True,
            nullable=False,
        ),
    )
    component: "ComponentTable" = Relationship(back_populates="layout")
