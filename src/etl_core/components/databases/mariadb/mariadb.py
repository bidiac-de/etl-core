from pydantic import Field

from etl_core.components.databases.sql_database import SQLDatabaseComponent
from etl_core.components.databases.if_exists_strategy import DatabaseOperation


class MariaDBComponent(SQLDatabaseComponent):
    """Base class for MariaDB components with common functionality."""

    charset: str = Field(default="utf8mb4", description="Character set for MariaDB")
    collation: str = Field(
        default="utf8mb4_unicode_ci", description="Collation for MariaDB"
    )

    # Database operation type
    operation: str = Field(
        default=DatabaseOperation.INSERT.value,
        description=(
            "Database operation type: insert, upsert, truncate, or update"
        ),
    )

    def _build_query(
        self, table: str, columns: list, operation: str, **kwargs
    ) -> str:
        """
        Build MariaDB-specific query based on operation type.

        Args:
            table: Target table name
            columns: List of column names
            operation: Database operation type
            **kwargs: Additional parameters (e.g., update_columns for upsert)

        Returns:
            SQL query string
        """
        columns_str = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])

        if operation == DatabaseOperation.TRUNCATE.value:
            # Clear table first, then insert
            return f"TRUNCATE TABLE {table}; INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        
        elif operation == DatabaseOperation.UPSERT.value:
            # Insert or update on duplicate key
            update_columns = kwargs.get("update_columns", columns)
            update_clause = ", ".join(
                [f"{col} = VALUES({col})" for col in update_columns]
            )
            return f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_clause}"
        
        elif operation == DatabaseOperation.UPDATE.value:
            # Pure update operation
            where_conditions = kwargs.get("where_conditions", [])
            if not where_conditions:
                raise ValueError("UPDATE operation requires where_conditions")
            
            set_clause = ", ".join([f"{col} = :{col}" for col in columns])
            where_clause = " AND ".join(where_conditions)
            return f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        
        else:  # INSERT (default)
            return f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"

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
