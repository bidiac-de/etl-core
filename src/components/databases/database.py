# src/components/databases/database.py
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Dict, Any

import pandas as pd
import dask.dataframe as dd
from pydantic import Field, model_validator

from src.components.base_component import Component, StrategyType, get_strategy
from src.components.column_definition import ColumnDefinition
from src.context.context import Context
from src.components.databases.sql_connection_handler import SQLConnectionHandler
from src.components.databases.pool_args import build_sql_engine_kwargs


class DatabaseComponent(Component, ABC):
    """
    Base class for database components (Mongo, SQL, ...).

    Shared, engine-agnostic tuning knobs live here so all receivers/components
    can use them consistently.
    """

    # Database-specific fields
    host: str = Field(..., description="Database host")
    port: int = Field(default=3306, description="Database port")
    database: str = Field(..., description="Database name")
    table: str = Field(..., description="Table name for operations")
    query: str = Field(default="", description="SQL query for read operations")
    credentials_id: int = Field(..., description="ID of credentials to use")

    # Engine-agnostic entity name (table / collection / view)
    entity_name: str = Field(
        default="",
        description="name of the target entity (table/collection).",
    )

    # Shared throughput knobs
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

    # Private attributes
    _connection_handler: SQLConnectionHandler = None
    _context: Context = None
    _receiver: Any = None
    _strategy: Any = None

    @model_validator(mode="after")
    def _build_objects(self):
        """Build database-specific objects after validation."""
        # Create connection handler without credentials initially
        # We'll create it later when context is available
        self._connection_handler = None

        # Create receiver (will be updated when connection is available)
        self._receiver = None

        # Set strategy based on component type
        if hasattr(self, "strategy_type"):
            self._strategy = get_strategy(self.strategy_type)
        else:
            # Default to bulk strategy for database operations
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
        # Allow Mock objects for testing
        if hasattr(value, "get_credentials"):  # Check if it has the required method
            self._context = value
        else:
            raise TypeError("context must have get_credentials method")

    @property
    def schema_definition(self) -> List[ColumnDefinition]:
        # This will be implemented by concrete components
        return []

    def _get_credentials(self) -> Dict[str, Any]:
        """Get credentials from context."""
        if not self._context:
            raise ValueError("Context not set for database component")

        # Get credentials from context
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

        # Create connection handler with credentials
        self._connection_handler = SQLConnectionHandler()
        url = SQLConnectionHandler.build_url(
            db_type="mariadb",
            user=creds["user"],
            password=creds["password"],
            host=self.host,
            port=self.port,
            database=creds["database"],
        )

        # Build engine kwargs from credentials pool settings
        credentials_obj = self._context.get_credentials(self.credentials_id)
        engine_kwargs = build_sql_engine_kwargs(credentials_obj)

        self._connection_handler.connect(url=url, engine_kwargs=engine_kwargs)

        # Create receiver with new connection
        self._receiver = self._create_receiver()

    def _create_receiver(self):
        """Create the appropriate receiver for this database type."""
        # This should be implemented by concrete subclasses
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
