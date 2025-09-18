from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict

import pandas as pd
import dask.dataframe as dd
from pydantic import Field, PrivateAttr, model_validator

from etl_core.components.databases.database import DatabaseComponent
from etl_core.components.databases.sql_connection_handler import (
    SQLConnectionHandler,
)
from etl_core.components.databases.pool_args import build_sql_engine_kwargs


class SQLDatabaseComponent(DatabaseComponent, ABC):
    """
    Base class for SQL database components (MariaDB, PostgreSQL, MySQL, etc.).

    This class provides SQL-specific functionality and abstracts away
    database-specific differences while maintaining the common interface.
    """

    charset: str = Field(default="utf8", description="Character set for SQL database")
    collation: str = Field(default="", description="Collation for SQL database")

    entity_name: str = Field(..., description="Name of the target entity (table/view)")

    _connection_handler: SQLConnectionHandler = None
    _log: logging.Logger = PrivateAttr(
        default_factory=lambda: logging.getLogger(
            "etl_core.components.databases.sql"
        )
    )

    @model_validator(mode="after")
    def _build_objects(self):
        """Build SQL database-specific objects after validation."""
        self._setup_connection()
        return self

    @property
    def connection_handler(self) -> SQLConnectionHandler:
        if self._connection_handler is None:
            self._log.debug("%s: connection handler missing; rebuilding.", self.name)
            self._setup_connection()
        if self._connection_handler is None:
            raise RuntimeError(f"{self.name}: SQL connection handler not initialized.")
        return self._connection_handler

    def _setup_connection(self):
        """Setup the SQL database connection with credentials and specific settings."""

        creds = self._get_credentials()

        self._connection_handler = SQLConnectionHandler()

        # Use comp_type directly
        url = SQLConnectionHandler.build_url(
            comp_type=self.comp_type,
            user=creds["user"],
            password=creds["password"],
            host=creds["host"],
            port=creds["port"],
            database=creds["database"],
        )

        engine_kwargs = build_sql_engine_kwargs(self._credentials)

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

    def cleanup_after_execution(self, force: bool = False) -> None:
        handler = self._connection_handler
        if handler is None:
            return

        try:
            closed = handler.close_pool(force=force)
            if not closed and not force:
                self._log.debug(
                    "%s: SQL pool still leased; forcing close.", self.name
                )
                closed = handler.close_pool(force=True)
        except Exception:
            self._log.exception(
                "%s: error while closing SQL connection pool.", self.name
            )
            closed = False

        if closed:
            self._log.debug("%s: SQL connection pool closed after execution.", self.name)
            self._connection_handler = None
        else:
            self._log.debug(
                "%s: SQL connection pool not closed; handler retained.", self.name
            )

    def __del__(self):
        """Cleanup connection when component is destroyed."""
        if getattr(self, "_connection_handler", None):
            try:
                self.cleanup_after_execution(force=True)
            except Exception:
                pass

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
