# src/components/databases/database.py
from __future__ import annotations

from abc import ABC
from typing import List

from pydantic import Field

from src.components.base_component import Component
from src.components.column_definition import ColumnDefinition
from src.context.context import Context


class DatabaseComponent(Component, ABC):
    """
    Base for all database components (Mongo, SQL, ...).

    Shared, engine-agnostic tuning knobs live here so all receivers/components
    can use them consistently.
    """

    context: Context = Field(default_factory=lambda: Context())
    schema_definition: List[ColumnDefinition] = Field(default_factory=list)

    # where to connect/look up credentials
    credentials_id: str = Field(
        default="",
        description="ID of the credentials in ContextRegistry",
    )

    # engine-agnostic entity name (table / collection / view)
    entity_name: str = Field(
        default="",
        description="name of the target entity (table/collection).",
    )

    # shared throughput knobs
    row_batch_size: int = Field(
        default=1_000,
        ge=1,
        description="Hint for server-side cursor batch size in row streaming.",
    )
    bulk_chunk_size: int = Field(
        default=50_000,
        ge=1,
        description="Records per chunk when writing/reading pandas DataFrames.",
    )
    bigdata_partition_chunk_size: int = Field(
        default=50_000,
        ge=1,
        description=(
            "Records per chunk inside each Dask partition for bigdata mode "
            "(used by readers/writers)."
        ),
    )
