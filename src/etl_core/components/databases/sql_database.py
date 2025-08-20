from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Any

import pandas as pd
import dask.dataframe as dd
from pydantic import Field, model_validator

from src.etl_core.components.databases.database import DatabaseComponent
from src.etl_core.components.databases.sql_connection_handler import SQLConnectionHandler
from src.etl_core.context.context import Context
from src.etl_core.components.component_registry import register_component


class SQLDatabaseComponent(DatabaseComponent, ABC):
    """
    Base class for SQL database components (MariaDB, PostgreSQL, MySQL, etc.).
    
    This class provides SQL-specific functionality and abstracts away
    database-specific differences while maintaining the common interface.
    """

    # SQL-specific fields
    charset: str = Field(default="utf8", description="Character set for SQL database")
    collation: str = Field(default="", description="Collation for SQL database")
    
    # Override table with entity_name for consistency
    entity_name: str = Field(..., description="Name of the target entity (table/view)")

    @model_validator(mode="after")
    def _build_objects(self):
        """Build SQL database-specific objects after validation."""
        # Call parent _build_objects first
        super()._build_objects()

        # Setup connection with credentials
        self._setup_connection()

        return self

    def _setup_connection(self):
        """Setup the SQL database connection with credentials and specific settings."""
        super()._setup_connection()

        # Additional SQL-specific connection setup can go here
        # For example, setting charset and collation
        if self._connection_handler and self.charset:
            try:
                # Set SQL-specific session variables using the connection handler
                with self._connection_handler.lease() as conn:
                    if self.charset:
                        conn.execute(f"SET NAMES {self.charset}")
                    if self.collation:
                        conn.execute(f"SET collation_connection = {self.collation}")
                    conn.commit()
            except Exception as e:
                # Log warning but don't fail
                print(f"Warning: Could not set SQL session variables: {e}")

    def _create_receiver(self):
        """Create the SQL database receiver."""
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
