from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Any

import pandas as pd
import dask.dataframe as dd
from pydantic import Field, model_validator

from etl_core.components.databases.database import DatabaseComponent
from etl_core.components.databases.sql_connection_handler import (
    SQLConnectionHandler,
)
from etl_core.components.databases.pool_args import build_sql_engine_kwargs
from etl_core.components.databases.if_exists_strategy import IfExistsStrategy


class SQLDatabaseComponent(DatabaseComponent, ABC):
    """
    Base class for SQL database components (MariaDB, PostgreSQL, MySQL, etc.).

    This class provides SQL-specific functionality and abstracts away
    database-specific differences while maintaining the common interface.
    """

    charset: str = Field(default="utf8", description="Character set for SQL database")
    collation: str = Field(default="", description="Collation for SQL database")

    # ✅ NEU: if_exists Parameter für alle SQL-Datenbanken
    if_exists: IfExistsStrategy = Field(
        default=IfExistsStrategy.APPEND,
        description="How to behave if the table already exists",
    )

    entity_name: str = Field(..., description="Name of the target entity (table/view)")

    _connection_handler: SQLConnectionHandler = None

    @model_validator(mode="after")
    def _build_objects(self):
        """Build SQL database-specific objects after validation."""
        self._setup_connection()
        return self

    @property
    def connection_handler(self) -> SQLConnectionHandler:
        return self._connection_handler

    def _setup_connection(self):
        """Setup the SQL database connection with credentials and specific settings."""
        if not self._context:
            return

        creds = self._get_credentials()

        self._connection_handler = SQLConnectionHandler()

        # Direct usage of comp_type - no mapping logic needed!
        url = SQLConnectionHandler.build_url(
            db_type=self.comp_type,
            user=creds["user"],
            password=creds["password"],
            host=creds["host"],
            port=creds["port"],
            database=creds["database"],
        )

        credentials_obj = self._context.get_credentials(self.credentials_id)
        engine_kwargs = build_sql_engine_kwargs(credentials_obj)

        self._connection_handler.connect(url=url, engine_kwargs=engine_kwargs)

        # Force subclasses to set their own session variables
        self._setup_session_variables()

    @abstractmethod
    def _setup_session_variables(self):
        """
        Setup database-specific session variables.
        Must be implemented by subclasses (MariaDB, PostgreSQL, etc.).
        """
        raise NotImplementedError

    def __del__(self):
        """Cleanup connection when component is destroyed."""
        if hasattr(self, "_connection_handler") and self._connection_handler:
            self._connection_handler.close_pool(force=True)

    def _build_insert_query(
        self, table: str, columns: list, if_exists: IfExistsStrategy
    ) -> str:
        """
        Build INSERT query based on if_exists strategy.

        Args:
            table: Target table name
            columns: List of column names
            if_exists: Strategy for handling existing data

        Returns:
            SQL query string
        """
        columns_str = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])
        base_query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"

        if if_exists == IfExistsStrategy.TRUNCATE:
            # For truncate, we'll handle this separately in the receiver
            return base_query
        elif if_exists == IfExistsStrategy.REPLACE:
            # Use REPLACE instead of INSERT
            return f"REPLACE INTO {table} ({columns_str}) VALUES ({placeholders})"
        elif if_exists == IfExistsStrategy.FAIL:
            # Simple INSERT that will fail on conflicts
            return base_query
        else:  # APPEND and others
            return base_query

    def _build_upsert_query(
        self, table: str, columns: list, if_exists: IfExistsStrategy, **kwargs
    ) -> str:
        """
        Build UPSERT query based on database-specific if_exists strategy.
        Must be implemented by subclasses for database-specific syntax.

        Args:
            table: Target table name
            columns: List of column names
            if_exists: Strategy for handling existing data
            **kwargs: Additional database-specific parameters

        Returns:
            SQL query string
        """
        # Base implementation - subclasses should override
        return self._build_insert_query(table, columns, if_exists)

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
