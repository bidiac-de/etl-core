from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Any

import pandas as pd
import dask.dataframe as dd
from pydantic import Field, model_validator

from etl_core.components.base_component import Component
from etl_core.context.context import Context


class DatabaseComponent(Component, ABC):
    """
    Base class for database components (Mongo, SQL, ...).

    Shared, engine-agnostic tuning knobs live here so all receivers/components
    can use them consistently.
    """

    ICON = "fa-solid fa-database"

    credentials_id: int = Field(..., description="ID of credentials to use")

    entity_name: str = Field(
        default="",
        description="name of the target entity (table/collection).",
    )

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

    _context: Context = None
    _receiver: Any = None

    @model_validator(mode="after")
    def _build_objects(self):
        """Build database-specific objects after validation."""
        self._receiver = None
        return self

    @property
    def context(self) -> Context:
        return self._context

    @context.setter
    def context(self, value: Context):
        if hasattr(value, "get_credentials"):
            self._context = value
        else:
            raise TypeError("context must have get_credentials method")

    def _get_credentials(self) -> Dict[str, Any]:
        """Get credentials from context."""
        if not self._context:
            raise ValueError("Context not set for database component")

        credentials = self._context.get_credentials(self.credentials_id)
        if not credentials:
            raise ValueError(f"Credentials with ID {self.credentials_id} not found")

        return {
            "user": credentials.get_parameter("user"),
            "password": credentials.decrypted_password,
            "database": credentials.get_parameter("database"),
            "host": credentials.get_parameter("host"),
            "port": credentials.get_parameter("port"),
        }

    @abstractmethod
    async def process_row(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """Process a single row. Implement in subclass."""
        raise NotImplementedError

    @abstractmethod
    async def process_bulk(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """Process an in-memory batch. Implement in subclass."""
        raise NotImplementedError

    @abstractmethod
    async def process_bigdata(self, *args: Any, **kwargs: Any) -> dd.DataFrame:
        """
        Stream-processing for big data. Implement in subclass.
        Should be a generator to avoid materializing large data.
        """
        raise NotImplementedError
