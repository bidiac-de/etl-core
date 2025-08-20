from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Any

import pandas as pd
import dask.dataframe as dd
from pydantic import Field, model_validator

from src.etl_core.components.databases.database import DatabaseComponent
from src.etl_core.components.databases.sql_connection_handler import SQLConnectionHandler
from src.etl_core.components.databases.pool_args import build_sql_engine_kwargs


class SQLDatabaseComponent(DatabaseComponent, ABC):
    """
    Base class for SQL database components (MariaDB, PostgreSQL, MySQL, etc.).

    This class provides SQL-specific functionality and abstracts away
    database-specific differences while maintaining the common interface.
    """

    query: str = Field(default="", description="SQL query for read operations")
    charset: str = Field(default="utf8", description="Character set for SQL database")
    collation: str = Field(default="", description="Collation for SQL database")

    entity_name: str = Field(..., description="Name of the target entity (table/view)")

    _connection_handler: SQLConnectionHandler = None

    @model_validator(mode="after")
    def _build_objects(self):
        """Build SQL database-specific objects after validation."""
        super()._build_objects()
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
        url = SQLConnectionHandler.build_url(
            db_type="mariadb",
            user=creds["user"],
            password=creds["password"],
            host=creds["host"],
            port=creds["port"],
            database=creds["database"],
        )

        credentials_obj = self._context.get_credentials(self.credentials_id)
        engine_kwargs = build_sql_engine_kwargs(credentials_obj)

        self._connection_handler.connect(url=url, engine_kwargs=engine_kwargs)
        self._receiver = self._create_receiver()

        if self._connection_handler and self.charset:
            try:
                with self._connection_handler.lease() as conn:
                    if self.charset:
                        conn.execute(f"SET NAMES {self.charset}")
                    if self.collation:
                        conn.execute(f"SET collation_connection = {self.collation}")
                    conn.commit()
            except Exception as e:
                print(f"Warning: Could not set SQL session variables: {e}")

    def _create_receiver(self):
        """Create the SQL database receiver."""
        raise NotImplementedError("Subclasses must implement _create_receiver")

    def __del__(self):
        """Cleanup connection when component is destroyed."""
        if hasattr(self, "_connection_handler") and self._connection_handler:
            self._connection_handler.close_pool(force=True)

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
