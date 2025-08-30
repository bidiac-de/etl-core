from pydantic import Field

from etl_core.components.databases.sql_database import SQLDatabaseComponent
from etl_core.components.databases.if_exists_strategy import DatabaseOperation


class PostgreSQLComponent(SQLDatabaseComponent):
    """Base class for PostgreSQL components with common functionality."""

    charset: str = Field(default="utf8", description="Character set for PostgreSQL")
    collation: str = Field(
        default="en_US.UTF-8", description="Collation for PostgreSQL"
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
        Build PostgreSQL-specific query based on operation type.

        Args:
            table: Target table name
            columns: List of column names
            operation: Database operation type
            **kwargs: Additional parameters
            (e.g., conflict_columns, update_columns for ON CONFLICT)

        Returns:
            SQL query string
        """
        columns_str = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])

        if operation == DatabaseOperation.TRUNCATE.value:
            # Clear table first, then insert
            return f"TRUNCATE TABLE {table}; INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        
        elif operation == DatabaseOperation.UPSERT.value:
            # Insert or update on conflict
            conflict_columns = kwargs.get("conflict_columns", ["id"])
            update_columns = kwargs.get("update_columns", columns)
            conflict_str = ", ".join(conflict_columns)
            update_clause = ", ".join(
                [f"{col} = EXCLUDED.{col}" for col in update_columns]
            )
            return (
                f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders}) "
                f"ON CONFLICT ({conflict_str}) DO UPDATE SET {update_clause}"
            )
        
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
        """Setup PostgreSQL-specific session variables."""
        if not self._connection_handler or not self.charset:
            return

        try:
            with self._connection_handler.lease() as conn:
                if self.charset:
                    conn.execute(f"SET client_encoding = '{self.charset}'")
                if self.collation:
                    conn.execute(f"SET lc_collate = '{self.collation}'")
                conn.commit()
        except Exception as e:
            print(f"Warning: Could not set PostgreSQL session variables: {e}")

    def _build_objects(self):
        """Build PostgreSQL-specific objects after validation."""
        super()._build_objects()
        # Set session variables after connection is established
        self._setup_session_variables()
        return self
