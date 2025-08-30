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
    MariaDBIfExistsStrategy,
)


class MariaDBReceiver(SQLReceiver):
    """MariaDB receiver for database operations."""

    def _build_upsert_query(
        self, table: str, columns: list, if_exists: str, **kwargs
    ) -> str:
        """
        Build MariaDB-specific UPSERT query based on if_exists strategy.

        Args:
            table: Target table name
            columns: List of column names
            if_exists: MariaDB-specific strategy for handling existing data
            **kwargs: Additional parameters
            (e.g., update_columns for ON DUPLICATE KEY UPDATE)

        Returns:
            SQL query string
        """
        columns_str = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])
        base_query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"

        if if_exists == MariaDBIfExistsStrategy.IGNORE:
            # MariaDB: INSERT IGNORE
            return (
                f"INSERT IGNORE INTO {table} ({columns_str}) "
                f"VALUES ({placeholders})"
            )
        elif if_exists == MariaDBIfExistsStrategy.ON_DUPLICATE_UPDATE:
            # MariaDB: ON DUPLICATE KEY UPDATE
            update_columns = kwargs.get("update_columns", columns)
            update_clause = ", ".join(
                [f"{col} = VALUES({col})" for col in update_columns]
            )
            return f"{base_query} ON DUPLICATE KEY UPDATE {update_clause}"
        else:
            # Fall back to base implementation for standard strategies
            if_exists_enum = IfExistsStrategy(if_exists)
            if if_exists_enum == IfExistsStrategy.TRUNCATE:
                # For truncate, we'll handle this separately
                return base_query
            elif if_exists_enum == IfExistsStrategy.REPLACE:
                # Use REPLACE instead of INSERT
                return (
                    f"REPLACE INTO {table} ({columns_str}) " f"VALUES ({placeholders})"
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
        **driver_kwargs: Any,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Yield MariaDB rows as dictionaries from a query."""
        query = driver_kwargs.get("query", f"SELECT * FROM {entity_name}")
        params = driver_kwargs.get("params", {})

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
        **driver_kwargs: Any,
    ) -> pd.DataFrame:
        """Read query results as a pandas DataFrame."""
        query = driver_kwargs.get("query", f"SELECT * FROM {entity_name}")
        params = driver_kwargs.get("params", {})

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
        **driver_kwargs: Any,
    ) -> dd.DataFrame:
        """Read large query results as a Dask DataFrame."""
        query = driver_kwargs.get("query", f"SELECT * FROM {entity_name}")
        params = driver_kwargs.get("params", {})

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
        **driver_kwargs: Any,
    ) -> Dict[str, Any]:
        """Write a single row and return the result."""
        query = driver_kwargs.get("query")
        table = driver_kwargs.get("table", entity_name)
        if_exists = driver_kwargs.get("if_exists", "append")

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
        **driver_kwargs: Any,
    ) -> pd.DataFrame:
        """Write a pandas DataFrame and return it."""
        if frame.empty:
            return frame

        query = driver_kwargs.get("query")
        table = driver_kwargs.get("table", entity_name)
        if_exists = driver_kwargs.get("if_exists", "append")

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
        **driver_kwargs: Any,
    ) -> dd.DataFrame:
        """Write a Dask DataFrame and return it."""
        query = driver_kwargs.get("query")
        table = driver_kwargs.get("table", entity_name)
        if_exists = driver_kwargs.get("if_exists", "append")

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
