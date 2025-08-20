from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Any

import pandas as pd
import dask.dataframe as dd
from pydantic import Field, model_validator

from src.etl_core.components.base_component import Component, StrategyType, get_strategy
from src.etl_core.context.context import Context
from src.etl_core.components.databases.sql_connection_handler import (
    SQLConnectionHandler,
)
from src.etl_core.components.databases.pool_args import build_sql_engine_kwargs


class DatabaseComponent(Component, ABC):
    """
    Base class for database components (Mongo, SQL, ...).

    Shared, engine-agnostic tuning knobs live here so all receivers/components
    can use them consistently.
    """

    host: str = Field(..., description="Database host")
    port: int = Field(default=3306, description="Database port")
    database: str = Field(..., description="Database name")
    query: str = Field(default="", description="SQL query for read operations")
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

    _connection_handler: SQLConnectionHandler = None
    _context: Context = None
    _receiver: Any = None
    _strategy: Any = None

    @model_validator(mode="after")
    def _build_objects(self):
        """Build database-specific objects after validation."""
        self._connection_handler = None
        self._receiver = None

        if hasattr(self, "strategy_type"):
            self._strategy = get_strategy(self.strategy_type)
        else:
            self._strategy = get_strategy(StrategyType.BULK)

        return self

    @property
    def connection_handler(self) -> SQLConnectionHandler:
        return self._connection_handler

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
        }

    def _setup_connection(self):
        """Setup the database connection with credentials."""
        if not self._context:
            return

        creds = self._get_credentials()

        self._connection_handler = SQLConnectionHandler()
        url = SQLConnectionHandler.build_url(
            db_type="mariadb",
            user=creds["user"],
            password=creds["password"],
            host=self.host,
            port=self.port,
            database=creds["database"],
        )

        credentials_obj = self._context.get_credentials(self.credentials_id)
        engine_kwargs = build_sql_engine_kwargs(credentials_obj)

        self._connection_handler.connect(url=url, engine_kwargs=engine_kwargs)
        self._receiver = self._create_receiver()

    def _create_receiver(self):
        """Create the appropriate receiver for this database type."""
        raise NotImplementedError("Subclasses must implement _create_receiver")

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

    def __del__(self):
        """Cleanup connection when component is destroyed."""
        if hasattr(self, "_connection_handler") and self._connection_handler:
            self._connection_handler.close_pool(force=True)
