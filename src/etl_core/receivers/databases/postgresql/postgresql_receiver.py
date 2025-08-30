from __future__ import annotations

import asyncio
from typing import Any, Dict, AsyncIterator

import pandas as pd
import dask.dataframe as dd
from sqlalchemy import text

from etl_core.components.databases.sql_connection_handler import SQLConnectionHandler
from etl_core.receivers.databases.sql_receiver import SQLReceiver

from etl_core.components.databases.if_exists_strategy import (
    IfExistsStrategy,
    PostgreSQLIfExistsStrategy,
)


class PostgreSQLReceiver(SQLReceiver):
    """PostgreSQL receiver for database operations."""

    def _build_upsert_query(
        self, table: str, columns: list, if_exists: str, **kwargs
    ) -> str:
        """
        Build PostgreSQL-specific UPSERT query based on if_exists strategy.

        Args:
            table: Target table name
            columns: List of column names
            if_exists: PostgreSQL-specific strategy for handling existing data
            **kwargs: Additional parameters (e.g., conflict_columns for ON CONFLICT)

        Returns:
            SQL query string
        """
        columns_str = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])
        base_query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"

        if if_exists == PostgreSQLIfExistsStrategy.ON_CONFLICT_DO_NOTHING:
            # PostgreSQL: ON CONFLICT DO NOTHING
            conflict_columns = kwargs.get(
                "conflict_columns", ["id"]
            )  # Default to 'id' column
            conflict_str = ", ".join(conflict_columns)
            return f"{base_query} ON CONFLICT ({conflict_str}) DO NOTHING"
        elif if_exists == PostgreSQLIfExistsStrategy.ON_CONFLICT_UPDATE:
            # PostgreSQL: ON CONFLICT DO UPDATE
            conflict_columns = kwargs.get("conflict_columns", ["id"])
            update_columns = kwargs.get("update_columns", columns)
            conflict_str = ", ".join(conflict_columns)
            update_clause = ", ".join(
                [f"{col} = EXCLUDED.{col}" for col in update_columns]
            )
            return (
                f"{base_query} ON CONFLICT ({conflict_str}) "
                f"DO UPDATE SET {update_clause}"
            )
        else:
            # Fall back to base implementation for standard strategies
            if_exists_enum = IfExistsStrategy(if_exists)
            if if_exists_enum == IfExistsStrategy.TRUNCATE:
                # For truncate, we'll handle this separately
                return base_query
            elif if_exists_enum == IfExistsStrategy.REPLACE:
                # PostgreSQL doesn't have REPLACE, use ON CONFLICT DO UPDATE instead
                conflict_columns = kwargs.get("conflict_columns", ["id"])
                conflict_str = ", ".join(conflict_columns)
                update_columns = kwargs.get("update_columns", columns)
                update_clause = ", ".join(
                    [f"{col} = EXCLUDED.{col}" for col in update_columns]
                )
                return (
                    f"{base_query} ON CONFLICT ({conflict_str}) "
                    f"DO UPDATE SET {update_clause}"
                )
            elif if_exists_enum == IfExistsStrategy.FAIL:
                # Simple INSERT that will fail on conflicts
                return base_query
            else:  # APPEND and others
                return base_query

    async def read_row(
        self,
        *,
        entity_name: str,
        metrics: Any,
        connection_handler: SQLConnectionHandler,
        batch_size: int = 1000,
        query: str | None = None,
        params: Dict[str, Any] | None = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Yield PostgreSQL rows as dictionaries from a query."""
        query = query or f"SELECT * FROM {entity_name}"
        params = params or {}

        def _execute_query():
            with connection_handler.lease() as conn:
                result = conn.execute(text(query), params)
                return [dict(row._mapping) for row in result]

        rows = await asyncio.to_thread(_execute_query)
        for row in rows:
            yield row

    async def read_bulk(
        self,
        *,
        entity_name: str,
        metrics: Any,
        connection_handler: SQLConnectionHandler,
        query: str | None = None,
        params: Dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Read query results as a pandas DataFrame."""
        query = query or f"SELECT * FROM {entity_name}"
        params = params or {}

        def _execute_query():
            with connection_handler.lease() as conn:
                result = conn.execute(text(query), params)
                return pd.DataFrame([dict(row._mapping) for row in result])

        return await asyncio.to_thread(_execute_query)

    async def read_bigdata(
        self,
        *,
        entity_name: str,
        metrics: Any,
        connection_handler: SQLConnectionHandler,
        query: str | None = None,
        params: Dict[str, Any] | None = None,
    ) -> dd.DataFrame:
        """Read large query results as a Dask DataFrame."""
        query = query or f"SELECT * FROM {entity_name}"
        params = params or {}

        def _execute_query():
            with connection_handler.lease() as conn:
                result = conn.execute(text(query), params)
                df = pd.DataFrame([dict(row._mapping) for row in result])
                return dd.from_pandas(df, npartitions=1)

        return await asyncio.to_thread(_execute_query)

    async def write_row(
        self,
        *,
        entity_name: str,
        row: Dict[str, Any],
        metrics: Any,
        connection_handler: SQLConnectionHandler,
        query: str | None = None,
        table: str | None = None,
        if_exists: str = "append",
    ) -> Dict[str, Any]:
        """Write a single row and return the result."""
        table = table or entity_name

        if not query:
            # Auto-generate query if none provided
            columns = list(row.keys())
            query = self._build_upsert_query(table, columns, if_exists)

        def _execute_query():
            with connection_handler.lease() as conn:
                result = conn.execute(text(query), row)
                conn.commit()
                return {"affected_rows": result.rowcount, "row": row}

        return await asyncio.to_thread(_execute_query)

    async def write_bulk(
        self,
        *,
        entity_name: str,
        frame: pd.DataFrame,
        metrics: Any,
        connection_handler: SQLConnectionHandler,
        query: str | None = None,
        table: str | None = None,
        if_exists: str = "append",
    ) -> pd.DataFrame:
        """Write a pandas DataFrame and return it."""
        if frame.empty:
            return frame

        table = table or entity_name

        if not query:
            # Auto-generate query if none provided
            columns = list(frame.columns)
            query = self._build_upsert_query(table, columns, if_exists)

        def _execute_query():
            with connection_handler.lease() as conn:
                # Execute custom query for each row
                for _, row in frame.iterrows():
                    conn.execute(text(query), row.to_dict())
                conn.commit()
                return frame

        return await asyncio.to_thread(_execute_query)

    async def write_bigdata(
        self,
        *,
        entity_name: str,
        frame: dd.DataFrame,
        metrics: Any,
        connection_handler: SQLConnectionHandler,
        query: str | None = None,
        table: str | None = None,
        if_exists: str = "append",
    ) -> dd.DataFrame:
        """Write a Dask DataFrame and return it."""
        table = table or entity_name

        if not query:
            # Auto-generate query if none provided
            # Get columns from first partition
            first_partition = (
                frame.map_partitions(lambda pdf: pdf).partitions[0].compute()
            )
            columns = list(first_partition.columns)
            query = self._build_upsert_query(table, columns, if_exists)

        def _execute_query():
            with connection_handler.lease() as conn:
                # Execute custom query for each partition
                for partition in frame.map_partitions(lambda pdf: pdf).partitions:
                    pdf = partition.compute()
                    if not pdf.empty:
                        for _, row in pdf.iterrows():
                            conn.execute(text(query), row.to_dict())
                conn.commit()
                return frame

        return await asyncio.to_thread(_execute_query)
