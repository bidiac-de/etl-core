from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Any

import pandas as pd
import dask.dataframe as dd
from pydantic import Field, model_validator

from src.etl_core.components.databases.database import DatabaseComponent
from src.etl_core.components.databases.sql_connection_handler import (
    SQLConnectionHandler,
)
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
        
        # Determine database type from comp_type
        db_type = self._get_db_type_from_comp_type()
        
        # Build connection URL
        url = SQLConnectionHandler.build_url(
            db_type=db_type,
            user=creds["user"],
            password=creds["password"],
            host=creds["host"],
            port=creds["port"],
            database=creds["database"],
        )

        credentials_obj = self._context.get_credentials(self.credentials_id)
        engine_kwargs = build_sql_engine_kwargs(credentials_obj)

        self._connection_handler.connect(url=url, engine_kwargs=engine_kwargs)
        
        # Set session variables based on database type
        self._setup_session_variables(db_type)

    def _get_db_type_from_comp_type(self) -> str:
        """Determine database type from comp_type."""
        comp_type = self.comp_type.lower()
        
        if "mariadb" in comp_type:
            return "mariadb"
        elif "postgresql" in comp_type or "postgres" in comp_type:
            return "postgresql"
        elif "sqlexpress" in comp_type:
            return "sqlexpress"
        elif "firebase" in comp_type:
            return "firebase"
        else:
            raise ValueError(f"Unsupported database type in comp_type: '{self.comp_type}'. "
                           f"Supported types: mariadb, postgresql, mysql, sqlite")

    def _setup_session_variables(self, db_type: str):
        """Setup database-specific session variables."""
        if not self._connection_handler or not self.charset:
            return

        try:
            with self._connection_handler.lease() as conn:
                if db_type in ["mariadb", "mysql"]:
                    # MySQL/MariaDB specific session variables
                    if self.charset:
                        conn.execute(f"SET NAMES {self.charset}")
                    if self.collation:
                        conn.execute(f"SET collation_connection = {self.collation}")
                elif db_type == "postgresql":
                    # PostgreSQL specific session variables
                    if self.charset:
                        conn.execute(f"SET client_encoding = '{self.charset}'")
                    if self.collation:
                        conn.execute(f"SET lc_collate = '{self.collation}'")
                conn.commit()
        except Exception as e:
            print(f"Warning: Could not set SQL session variables: {e}")

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
