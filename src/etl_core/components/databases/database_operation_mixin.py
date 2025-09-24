from __future__ import annotations

from typing import Optional
from pydantic import Field

from etl_core.components.databases.if_exists_strategy import DatabaseOperation


class DatabaseOperationMixin:
    """
    Mixin class for database write operations.

    This mixin provides the operation field and basic query building functionality
    that only write components need. Read components should not inherit this.

    Subclasses should override _build_query to
    provide database-specific implementations.
    """

    where_conditions: Optional[list[str]] = Field(
        default_factory=list,
        description=(
            "WHERE conditions for UPDATE operations (required for update operation)"
        ),
    )

    operation: DatabaseOperation = Field(
        default=DatabaseOperation.INSERT,
        description="Database operation type: insert, upsert, truncate, or update",
    )

    def _build_query(
        self, table: str, columns: list, operation: DatabaseOperation, **kwargs
    ) -> str:
        """
        Build query based on operation type.

        This is a basic implementation that should be overridden by subclasses
        to provide database-specific SQL syntax.

        Args:
            table: Target table name
            columns: List of column names
            operation: Database operation type
            **kwargs: Additional parameters

        Returns:
            SQL query string
        """
        columns_str = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])

        if operation == DatabaseOperation.TRUNCATE:
            # Clear table first, then insert
            return f"TRUNCATE TABLE {table}; INSERT INTO {table} \
            ({columns_str}) VALUES ({placeholders})"

        elif operation == DatabaseOperation.UPSERT:
            # Default upsert behavior - subclasses should override
            return f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"

        elif operation == DatabaseOperation.UPDATE:
            # Pure update operation
            if not self.where_conditions:
                raise ValueError("UPDATE operation requires where_conditions")

            set_clause = ", ".join([f"{col} = :{col}" for col in columns])
            where_clause = " AND ".join(self.where_conditions)
            return f"UPDATE {table} SET {set_clause} WHERE {where_clause}"

        else:  # INSERT (default)
            return f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
