from __future__ import annotations

from typing import Any, AsyncIterator, Dict, Optional  # noqa: F401

import dask.dataframe as dd
import pandas as pd
from pydantic import Field, model_validator

from etl_core.components.databases.sqlserver.sqlserver import SQLServerComponent
from etl_core.components.databases.database_operation_mixin import (
    DatabaseOperationMixin,
)
from etl_core.components.databases.if_exists_strategy import DatabaseOperation
from etl_core.components.component_registry import register_component
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.databases.sqlserver.sqlserver_receiver import SQLServerReceiver
from etl_core.components.envelopes import Out
from etl_core.components.wiring.ports import InPortSpec, OutPortSpec


@register_component("write_sqlserver")
class SQLServerWrite(SQLServerComponent, DatabaseOperationMixin):
    """
    SQL Server writer with ports + schema.

    - INPUT_PORTS:
        - 'in' (required): rows/frames to write
    - OUTPUT_PORTS:
        - 'out' (optional): passthrough of what was written (useful for chaining/tests)
    """

    INPUT_PORTS = (InPortSpec(name="in", required=True, fanin="many"),)
    OUTPUT_PORTS = (OutPortSpec(name="out", required=False, fanout="many"),)

    def _build_query(
        self, table: str, columns: list, operation: DatabaseOperation, **kwargs
    ) -> str:
        """
        Build SQL Server-specific query based on operation type.

        Args:
            table: Target table name
            columns: List of column names
            operation: Database operation type
            **kwargs: Additional parameters
                (e.g., conflict_columns, update_columns for MERGE)

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
            # Use MERGE for upsert in SQL Server
            conflict_columns = kwargs.get("conflict_columns", ["id"])
            update_columns = kwargs.get("update_columns", columns)

            conflict_str = ", ".join(conflict_columns)
            update_clause = ", ".join(
                [f"target.{col} = source.{col}" for col in update_columns]
            )

            merge_query = f"""
            MERGE {table} AS target
            USING (SELECT {', '.join([f':{col} AS {col}' for col in columns])}) AS source
            ON {' AND '.join([f'target.{col} = source.{col}' for col in conflict_columns])}
            WHEN MATCHED THEN
                UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
                INSERT ({columns_str}) VALUES ({placeholders});
            """
            return merge_query.strip()

        elif operation == DatabaseOperation.UPDATE:
            # Pure update operation
            if not self.where_conditions:
                raise ValueError("UPDATE operation requires where_conditions")

            set_clause = ", ".join([f"{col} = :{col}" for col in columns])
            where_clause = " AND ".join(self.where_conditions)
            return f"UPDATE {table} SET {set_clause} WHERE {where_clause}"

        else:  # INSERT (default)
            return f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"

    @model_validator(mode="after")
    def _build_objects(self):
        """Build SQL Server-specific objects after validation."""
        super()._build_objects()
        self._receiver = SQLServerReceiver()
        schema = self.in_port_schemas["in"]
        columns = [field.name for field in schema.fields]
        self._query = self._build_query(self.entity_name, columns, self.operation)
        return self

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Write a single row and emit it (or receiver result) on 'out'."""
        result = await self._receiver.write_row(
            entity_name=self.entity_name,
            row=row,
            metrics=metrics,
            query=self._query,
            connection_handler=self.connection_handler,
        )
        yield Out(port="out", payload=result)

    async def process_bulk(
        self, data: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Write a pandas DataFrame and emit the same frame on 'out'."""
        result = await self._receiver.write_bulk(
            entity_name=self.entity_name,
            frame=data,
            metrics=metrics,
            query=self._query,
            connection_handler=self.connection_handler,
        )
        yield Out(port="out", payload=result)

    async def process_bigdata(
        self, ddf: dd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Write a Dask DataFrame and emit the same ddf on 'out'."""
        result = await self._receiver.write_bigdata(
            entity_name=self.entity_name,
            frame=ddf,
            metrics=metrics,
            query=self._query,
            connection_handler=self.connection_handler,
        )
        yield Out(port="out", payload=result)


SQLServerWrite.model_rebuild()
