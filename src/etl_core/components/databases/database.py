from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Optional, Any

import pandas as pd
import dask.dataframe as dd
from pydantic import Field, model_validator

from etl_core.components.base_component import Component
from etl_core.context.credentials import Credentials
from etl_core.persistance.handlers.credentials_handler import CredentialsHandler


class DatabaseComponent(Component, ABC):
    """
    Base class for database components (Mongo, SQL, ...).

    Shared, engine-agnostic tuning knobs live here so all receivers/components
    can use them consistently.
    """

    credentials_id: str = Field(..., description="ID of credentials to use")

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

    _credentials: Optional[Credentials] = None
    _receiver: Any = None

    @model_validator(mode="after")
    def _build_objects(self):
        """Build database-specific objects after validation."""
        self._receiver = None
        self._credentials = self._resolve_credentials()
        return self

    def _resolve_credentials(self) -> Credentials:
        """
        Hydrate credentials from persistence (DB + keyring).
        Raises if not found.
        """
        repo = CredentialsHandler()
        loaded = repo.get_by_id(self.credentials_id)
        if not loaded:
            raise ValueError(f"Credentials with ID {self.credentials_id} not found")
        creds, _ = loaded
        return creds

    def _get_credentials(self) -> Dict[str, Any]:
        """
        Return connection parameters. Lazily resolves credentials if needed.
        """
        if self._credentials is None:
            self._credentials = self._resolve_credentials()

        creds = self._credentials
        return {
            "user": creds.get_parameter("user"),
            "password": creds.decrypted_password,
            "database": creds.get_parameter("database"),
            "host": creds.get_parameter("host"),
            "port": creds.get_parameter("port"),
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
