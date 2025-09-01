from typing import Any, Optional, List
from uuid import uuid4

from sqlalchemy import Column, ForeignKey, UniqueConstraint, Integer, String
from sqlalchemy.types import JSON
from sqlmodel import Field, Relationship, SQLModel

from etl_core.persistance.base_models.component_base import ComponentBase
from etl_core.persistance.base_models.dataclasses_base import LayoutBase, MetaDataBase
from etl_core.persistance.base_models.job_base import JobBase

_FOREIGN_KEY_COMPONENT_TABLE = "componenttable.id"
_FOREIGN_KEY_JOB_TABLE = "jobtable.id"
_CASCADE_ALL = "all, delete-orphan"


class JobTable(JobBase, table=True):
    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)

    components: List["ComponentTable"] = Relationship(
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
    __table_args__ = (
        UniqueConstraint("job_id", "name", name="uq_components_job_name"),
    )

    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)

    job_id: str = Field(
        sa_column=Column(
            ForeignKey(_FOREIGN_KEY_JOB_TABLE, ondelete="CASCADE"),
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

    # wiring (edges)
    outgoing_links: List["ComponentLinkTable"] = Relationship(
        back_populates="src_component",
        sa_relationship_kwargs={
            "cascade": _CASCADE_ALL,
            "passive_deletes": True,
            "primaryjoin": "ComponentTable.id==ComponentLinkTable.src_component_id",
        },
    )
    incoming_links: List["ComponentLinkTable"] = Relationship(
        back_populates="dst_component",
        sa_relationship_kwargs={
            "cascade": _CASCADE_ALL,
            "passive_deletes": True,
            "primaryjoin": "ComponentTable.id==ComponentLinkTable.dst_component_id",
        },
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


class ComponentLinkTable(SQLModel, table=True):
    """
    Port-to-port wiring edge. Ordering is tracked by 'position' to keep
    targets stable per out_port when needed by the runtime/UI.
    """

    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)

    job_id: str = Field(
        sa_column=Column(
            ForeignKey(_FOREIGN_KEY_JOB_TABLE, ondelete="CASCADE"), nullable=False
        )
    )

    src_component_id: str = Field(
        sa_column=Column(
            ForeignKey(_FOREIGN_KEY_COMPONENT_TABLE, ondelete="CASCADE"), nullable=False
        )
    )
    src_out_port: str = Field(sa_column=Column(String, nullable=False))

    dst_component_id: str = Field(
        sa_column=Column(
            ForeignKey(_FOREIGN_KEY_COMPONENT_TABLE, ondelete="CASCADE"), nullable=False
        )
    )
    dst_in_port: str = Field(sa_column=Column(String, nullable=False))

    # preserve order among multiple edges from the same out port
    position: int = Field(
        default=0, sa_column=Column(Integer, nullable=False, default=0)
    )

    __table_args__ = (
        UniqueConstraint(
            "src_component_id",
            "src_out_port",
            "position",
            name="uq_links_src_port_position",
        ),
    )

    src_component: "ComponentTable" = Relationship(
        back_populates="outgoing_links",
        sa_relationship_kwargs={
            "primaryjoin": "ComponentLinkTable.src_component_id==ComponentTable.id",
        },
    )
    dst_component: "ComponentTable" = Relationship(
        back_populates="incoming_links",
        sa_relationship_kwargs={
            "primaryjoin": "ComponentLinkTable.dst_component_id==ComponentTable.id",
        },
    )


class MetaDataTable(MetaDataBase, table=True):
    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)

    job_id: Optional[str] = Field(
        default=None,
        sa_column=Column(
            ForeignKey(_FOREIGN_KEY_JOB_TABLE, ondelete="CASCADE"),
            unique=True,
            nullable=True,
        ),
    )
    component_id: Optional[str] = Field(
        default=None,
        sa_column=Column(
            ForeignKey(_FOREIGN_KEY_COMPONENT_TABLE, ondelete="CASCADE"),
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
            ForeignKey(_FOREIGN_KEY_COMPONENT_TABLE, ondelete="CASCADE"),
            unique=True,
            nullable=False,
        ),
    )
    component: "ComponentTable" = Relationship(back_populates="layout")
