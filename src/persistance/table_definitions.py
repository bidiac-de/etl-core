from typing import List
from uuid import uuid4

from sqlmodel import SQLModel, Field, Column
from sqlalchemy import JSON, Enum
from src.components.base_component import StrategyType


class JobTable(SQLModel, table=True):
    """
    SQLModel table definition mirroring the Job Pydantic model.
    """

    id: str = Field(
        default_factory=lambda: str(uuid4()),
        primary_key=True,
        index=True,
        nullable=False,
    )
    name: str = Field(default="default_job_name", nullable=False)
    num_of_retries: int = Field(default=0, nullable=False)
    file_logging: bool = Field(default=False, nullable=False)
    strategy_type: StrategyType = Field(
        default=StrategyType.ROW,
        sa_column=Column(Enum(StrategyType, name="strategytype_enum"), nullable=False),
    )
    components: List[dict] = Field(
        default_factory=list,
        sa_column=Column(JSON, nullable=False),
    )
    metadata_: dict = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False),
    )
