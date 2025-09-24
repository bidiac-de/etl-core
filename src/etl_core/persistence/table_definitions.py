from typing import Any, Optional, List
from uuid import uuid4

from sqlalchemy import Column, ForeignKey, UniqueConstraint, Integer, String
from sqlalchemy.types import JSON
from sqlmodel import Field, Relationship, SQLModel
from datetime import datetime

from etl_core.persistence.base_models.component_base import ComponentBase
from etl_core.persistence.base_models.dataclasses_base import LayoutBase, MetaDataBase
from etl_core.persistence.base_models.job_base import JobBase
from enum import Enum

_FOREIGN_KEY_COMPONENT_TABLE = "componenttable.id"
_FOREIGN_KEY_JOB_TABLE = "jobtable.id"
_CASCADE_ALL = "all, delete-orphan"
_FOREIGN_KEY_CONTEXT_PROVIDER = "contexttable.provider_id"


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


class CredentialsTable(SQLModel, table=True):
    """
    Persist non-secret parts of DB credentials.
    Secrets (passwords, secure params) stay in secret backend under id/*.
    """

    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True, index=True)
    name: str = Field(nullable=False)
    user: str = Field(nullable=False)
    host: str = Field(nullable=False)
    port: int = Field(nullable=False)
    database: str = Field(nullable=False)
    pool_max_size: Optional[int] = None
    pool_timeout_s: Optional[int] = None
    created_at: datetime = Field(default_factory=datetime.now, nullable=False)


class ContextTable(SQLModel, table=True):
    """
    Persist context metadata only.
    Secure params are tracked via ContextParameterTable rows; values live in
    the secret backend (value empty in DB for secure keys).
    """

    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True, index=True)
    name: str = Field(nullable=False)
    environment: str = Field(nullable=False)
    created_at: datetime = Field(default_factory=datetime.now, nullable=False)

    parameters: list["ContextParameterTable"] = Relationship(
        back_populates="context",
        sa_relationship_kwargs={
            "cascade": "all, delete-orphan",
            "passive_deletes": True,
        },
    )
    credentials_map: list["ContextCredentialsMapTable"] = Relationship(
        back_populates="context",
        sa_relationship_kwargs={
            "cascade": "all, delete-orphan",
            "passive_deletes": True,
        },
    )


class ContextParameterTable(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)

    context_id: str = Field(
        sa_column=Column(
            String,
            ForeignKey("contexttable.id", ondelete="CASCADE"),
            index=True,
            nullable=False,
        )
    )
    key: str = Field(index=True, nullable=False)
    value: str = Field(default="", nullable=False)
    is_secure: bool = Field(default=False, nullable=False)

    __table_args__ = (
        UniqueConstraint("context_id", "key", name="uq_contextparam_context_key"),
    )

    context: ContextTable | None = Relationship(back_populates="parameters")


class ContextCredentialsMapTable(SQLModel, table=True):
    """
    Env -> credentials mapping for a context.
    Rows are non-secret: they reference credentials ids.
    """

    id: int | None = Field(default=None, primary_key=True)

    context_id: str = Field(
        sa_column=Column(
            String,
            ForeignKey("contexttable.id", ondelete="CASCADE"),
            index=True,
            nullable=False,
        )
    )
    environment: str = Field(nullable=False)
    credentials_id: str = Field(
        sa_column=Column(
            String,
            ForeignKey("credentialstable.id", ondelete="RESTRICT"),
            index=True,
            nullable=False,
        )
    )

    __table_args__ = (
        UniqueConstraint("context_id", "environment", name="uq_ctxcred_context_env"),
    )

    context: "ContextTable" = Relationship(back_populates="credentials_map")


class ExecutionTable(SQLModel, table=True):
    """
    One persisted job execution (a run). Attempts are tracked separately.
    """

    id: str = Field(primary_key=True)  # we store the runtime execution.id here
    job_id: str = Field(
        sa_column=Column(
            ForeignKey("jobtable.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        )
    )
    environment: Optional[str] = Field(default=None, nullable=True, index=True)
    status: str = Field(default="RUNNING", nullable=False, index=True)
    error: Optional[str] = Field(default=None, nullable=True)
    started_at: datetime = Field(default_factory=datetime.now, nullable=False)
    finished_at: Optional[datetime] = Field(default=None, nullable=True)


class ExecutionAttemptTable(SQLModel, table=True):
    """
    A single attempt within an execution.
    """

    id: str = Field(primary_key=True)
    execution_id: str = Field(
        sa_column=Column(
            ForeignKey("executiontable.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        )
    )
    attempt_index: int = Field(nullable=False)
    status: str = Field(default="RUNNING", nullable=False, index=True)
    error: Optional[str] = Field(default=None, nullable=True)
    started_at: datetime = Field(default_factory=datetime.now, nullable=False)
    finished_at: Optional[datetime] = Field(default=None, nullable=True)


class TriggerType(str, Enum):
    INTERVAL = "interval"
    CRON = "cron"
    DATE = "date"


class ScheduleTable(SQLModel, table=True):
    """
    A persisted schedule definition for executing a job by name using
    APScheduler triggers. Trigger args are stored as JSON so we can
    support interval/cron/date uniformly.
    """

    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)
    name: str = Field(nullable=False, index=True)

    # reference job by id to avoid name lookups
    job_id: str = Field(
        sa_column=Column(
            ForeignKey(_FOREIGN_KEY_JOB_TABLE, ondelete="CASCADE"),
            nullable=False,
            index=True,
        )
    )

    # execution context gate (DEV/TEST/PROD)
    environment: str = Field(nullable=False, index=True)

    trigger_type: TriggerType = Field(sa_column=Column(String, nullable=False))
    trigger_args: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column(JSON, nullable=False)
    )

    # scheduling status
    is_paused: bool = Field(default=False, nullable=False)

    # audit fields
    created_at: datetime = Field(default_factory=datetime.now, nullable=False)
    updated_at: datetime = Field(default_factory=datetime.now, nullable=False)
