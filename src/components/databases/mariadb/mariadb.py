from abc import abstractmethod
from typing import Dict, Any, AsyncIterator
import pandas as pd
import dask.dataframe as dd
from pydantic import Field

from src.components.databases.database import DatabaseComponent


class MariaDBComponent(DatabaseComponent):
    """Base class for MariaDB components with common functionality."""
    
    # MariaDB-specific fields
    charset: str = Field(default="utf8mb4", description="Character set for MariaDB")
    collation: str = Field(default="utf8mb4_unicode_ci", description="Collation for MariaDB")
    
    def _setup_connection(self):
        """Setup the MariaDB connection with credentials and MariaDB-specific settings."""
        super()._setup_connection()
        
        # Additional MariaDB-specific connection setup can go here
        # For example, setting charset and collation
        if self._connection_handler and self._connection_handler.connection:
            try:
                # Set MariaDB-specific session variables
                conn = self._connection_handler.connection
                conn.execute(f"SET NAMES {self.charset}")
                conn.execute(f"SET collation_connection = {self.collation}")
                conn.commit()
            except Exception as e:
                # Log warning but don't fail
                print(f"Warning: Could not set MariaDB session variables: {e}")
