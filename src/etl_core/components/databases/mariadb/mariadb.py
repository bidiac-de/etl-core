from pydantic import Field

from etl_core.components.databases.sql_database import SQLDatabaseComponent, IfExistsStrategy


class MariaDBIfExistsStrategy(IfExistsStrategy):
    """MariaDB-specific strategies for handling existing tables."""
    IGNORE = "ignore"                           # MariaDB: IGNORE bei Duplikaten
    ON_DUPLICATE_UPDATE = "on_duplicate_update" # MariaDB: ON DUPLICATE KEY UPDATE


class MariaDBComponent(SQLDatabaseComponent):
    """Base class for MariaDB components with common functionality."""

    charset: str = Field(default="utf8mb4", description="Character set for MariaDB")
    collation: str = Field(
        default="utf8mb4_unicode_ci", description="Collation for MariaDB"
    )
    
    # MariaDB-spezifische if_exists Strategie mit intelligentem Standard
    if_exists: MariaDBIfExistsStrategy = Field(
        default=MariaDBIfExistsStrategy.APPEND, 
        description="MariaDB-specific behavior for existing tables (APPEND by default, but IGNORE and ON_DUPLICATE_UPDATE available)"
    )

    def _build_upsert_query(self, table: str, columns: list, if_exists: MariaDBIfExistsStrategy, **kwargs) -> str:
        """
        Build MariaDB-specific UPSERT query based on if_exists strategy.
        
        Args:
            table: Target table name
            columns: List of column names
            if_exists: MariaDB-specific strategy for handling existing data
            **kwargs: Additional parameters (e.g., update_columns for ON DUPLICATE KEY UPDATE)
            
        Returns:
            SQL query string
        """
        columns_str = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])
        base_query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        
        if if_exists == MariaDBIfExistsStrategy.IGNORE:
            # MariaDB: INSERT IGNORE
            return f"INSERT IGNORE INTO {table} ({columns_str}) VALUES ({placeholders})"
        elif if_exists == MariaDBIfExistsStrategy.ON_DUPLICATE_UPDATE:
            # MariaDB: ON DUPLICATE KEY UPDATE
            update_columns = kwargs.get('update_columns', columns)
            update_clause = ", ".join([f"{col} = VALUES({col})" for col in update_columns])
            return f"{base_query} ON DUPLICATE KEY UPDATE {update_clause}"
        else:
            # Fall back to base implementation for standard strategies
            return super()._build_upsert_query(table, columns, if_exists, **kwargs)

    def _setup_session_variables(self):
        """Setup MariaDB-specific session variables."""
        if not self._connection_handler or not self.charset:
            return

        try:
            with self._connection_handler.lease() as conn:
                if self.charset:
                    conn.execute(f"SET NAMES {self.charset}")
                if self.collation:
                    conn.execute(f"SET collation_connection = {self.collation}")
                conn.commit()
        except Exception as e:
            print(f"Warning: Could not set MariaDB session variables: {e}")

    def _build_objects(self):
        """Build MariaDB-specific objects after validation."""
        super()._build_objects()
        # Set session variables after connection is established
        self._setup_session_variables()
        return self
