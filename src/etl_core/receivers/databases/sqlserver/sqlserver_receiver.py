from __future__ import annotations

import asyncio
from typing import Any, Dict, AsyncIterator

import pandas as pd
import dask.dataframe as dd
from sqlalchemy import text

from etl_core.components.databases.sql_connection_handler import SQLConnectionHandler
from etl_core.receivers.databases.sql_receiver import SQLReceiver


class SQLServerReceiver(SQLReceiver):
    """SQL Server receiver for database operations."""

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
        """Yield SQL Server rows as dictionaries from a query."""
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
        query: str,  # Query is always required now
        table: str | None = None,
    ) -> Dict[str, Any]:
        """Write a single row and return the result."""
        table = table or entity_name

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
        query: str,  # Query is always required now
        table: str | None = None,
    ) -> pd.DataFrame:
        """Write a pandas DataFrame and return it."""
        if frame.empty:
            return frame

        table = table or entity_name

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
        query: str,  # Query is always required now
        table: str | None = None,
    ) -> dd.DataFrame:
        """Write a Dask DataFrame and return it."""
        table = table or entity_name

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
