from __future__ import annotations

import asyncio
from typing import Any, Dict, AsyncIterator, ClassVar

import pandas as pd
import dask.dataframe as dd
from sqlalchemy import text

from etl_core.components.databases.sql_connection_handler import SQLConnectionHandler
from etl_core.receivers.databases.sql_receiver import SQLReceiver


class MariaDBReceiver(SQLReceiver):
    """MariaDB receiver for database operations."""

    SQL_DIALECT: ClassVar[str] = "mysql+mysqlconnector"

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
        """Yield MariaDB rows as dictionaries from a query."""
        query = query or f"SELECT * FROM {entity_name}"
        params = params or {}

        def _execute_query():
            with connection_handler.lease() as conn:
                result = conn.execute(text(query), params)
                return [dict(row._mapping) for row in result]

        rows = await asyncio.to_thread(_execute_query)
        metrics.lines_received += len(rows)
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

        df = await asyncio.to_thread(_execute_query)
        metrics.lines_received += len(df)
        return df

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

        ddf = await asyncio.to_thread(_execute_query)
        # For BigData, we might not want to compute length as it triggers computation
        # But FilterReceiver does it with try-except. Let's follow that pattern if possible,
        # or at least count partitions?
        # The FilterReceiver creates a dask graph.
        # Here we are returning a dask dataframe.
        # Computing length of ddf might be expensive but required for metrics?
        # Let's try to be consistent with FilterReceiver but maybe safer.
        # Actually, reading *all* data into memory to count it defeats the purpose of BigData if we aren't careful.
        # But _execute_query above already reads it into pandas and converts to dask (npartitions=1).
        # So it IS in memory anyway in this implementation (which seems like a known limitation/choice in this receiver).
        # So we can safely count it.
        try:
            metrics.lines_received += len(ddf)
        except Exception:
            pass
        return ddf

    async def write_row(
        self,
        *,
        entity_name: str,
        row: Dict[str, Any],
        metrics: Any,
        connection_handler: SQLConnectionHandler,
        query: str,
        table: str | None = None,
    ) -> Dict[str, Any]:
        """Write a single row and return the result."""
        table = table or entity_name

        def _execute_query():
            with connection_handler.lease() as conn:
                result = conn.execute(text(query), row)
                conn.commit()
                return {"affected_rows": result.rowcount, "row": row}

        result = await asyncio.to_thread(_execute_query)
        metrics.lines_received += 1
        metrics.lines_forwarded += result["affected_rows"]
        return result

    async def write_bulk(
        self,
        *,
        entity_name: str,
        frame: pd.DataFrame,
        metrics: Any,
        connection_handler: SQLConnectionHandler,
        query: str,
        table: str | None = None,
    ) -> pd.DataFrame:
        """Write a pandas DataFrame and return it."""
        if frame.empty:
            return frame

        table = table or entity_name

        def _execute_query():
            with connection_handler.lease() as conn:
                for _, row in frame.iterrows():
                    conn.execute(text(query), row.to_dict())
                conn.commit()
                return frame

        result = await asyncio.to_thread(_execute_query)
        metrics.lines_received += len(frame)
        # assuming all succeeded if no error raised
        metrics.lines_forwarded += len(frame)
        return result

    async def write_bigdata(
        self,
        *,
        entity_name: str,
        frame: dd.DataFrame,
        metrics: Any,
        connection_handler: SQLConnectionHandler,
        query: str,
        table: str | None = None,
    ) -> dd.DataFrame:
        """Write a Dask DataFrame and return it."""
        table = table or entity_name

        def _execute_query():
            with connection_handler.lease() as conn:
                for partition in frame.map_partitions(lambda pdf: pdf).partitions:
                    pdf = partition.compute()
                    if not pdf.empty:
                        for _, row in pdf.iterrows():
                            conn.execute(text(query), row.to_dict())
                conn.commit()
                return frame

        result = await asyncio.to_thread(_execute_query)
        try:
            count = len(result)
            metrics.lines_received += count
            metrics.lines_forwarded += count
        except Exception:
            pass
        return result
