from __future__ import annotations

from typing import Optional  # noqa: F401
from pydantic import model_validator

from etl_core.components.component_registry import register_component
from etl_core.components.databases.mariadb.mariadb import MariaDBComponent
from etl_core.components.databases.database_operation_mixin import (
    DatabaseOperationMixin,
)
from etl_core.components.databases.sql_writer_base import SQLWriterBase
from etl_core.components.databases.if_exists_strategy import DatabaseOperation
from etl_core.components.wiring.ports import InPortSpec, OutPortSpec
from etl_core.receivers.databases.mariadb.mariadb_receiver import MariaDBReceiver


@register_component("write_mariadb")
class MariaDBWrite(SQLWriterBase, MariaDBComponent, DatabaseOperationMixin):
    """
    MariaDB writer with ports + schema.

    - INPUT_PORTS:
        - 'in' (required): rows/frames to write
    - OUTPUT_PORTS:
        - 'out' (optional): passthrough of what was written (useful for chaining/tests)
    """

    INPUT_PORTS = (InPortSpec(name="in", required=True, fanin="many"),)
    OUTPUT_PORTS = (OutPortSpec(name="out", required=False, fanout="many"),)

    receiver_class = MariaDBReceiver

    def _build_query(
        self, table: str, columns: list, operation: DatabaseOperation, **kwargs
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

        if operation == DatabaseOperation.TRUNCATE:
            # Clear table first, then insert
            return f"TRUNCATE TABLE {table}; INSERT INTO {table} \
            ({columns_str}) VALUES ({placeholders})"

        elif operation == DatabaseOperation.UPSERT:
            # Insert or update on duplicate key
            update_columns = kwargs.get("update_columns", columns)
            update_clause = ", ".join(
                [f"{col} = VALUES({col})" for col in update_columns]
            )
            return f"INSERT INTO {table} ({columns_str}) VALUES \
            ({placeholders}) ON DUPLICATE KEY UPDATE {update_clause}"

        elif operation == DatabaseOperation.UPDATE:
            # Pure update operation
            if not self.where_conditions:
                raise ValueError("UPDATE operation requires where_conditions")

            set_clause = ", ".join([f"{col} = :{col}" for col in columns])
            where_clause = " AND ".join(self.where_conditions)
            return f"UPDATE {table} SET {set_clause} WHERE {where_clause}"

        else:
            return f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"

    @model_validator(mode="after")
    def _build_objects(self):
        """Build MariaDB-specific objects after validation."""
        self._initialize_receiver()
        schema = self.in_port_schemas["in"]
        columns = [field.name for field in schema.fields]
        self._query = self._build_query(self.entity_name, columns, self.operation)
        return self


MariaDBWrite.model_rebuild()
