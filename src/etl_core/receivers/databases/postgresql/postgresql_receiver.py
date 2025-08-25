import asyncio
from typing import Any, Dict, AsyncIterator

import pandas as pd
import dask.dataframe as dd
from sqlalchemy import text

from etl_core.components.databases.sql_connection_handler import SQLConnectionHandler
from src.etl_core.receivers.databases.sql_receiver import SQLReceiver


class PostgreSQLReceiver(SQLReceiver):
    """Receiver for PostgreSQL operations supporting row, bulk, and bigdata modes."""

    async def read_row(
        self,
        *,
        entity_name: str,
        metrics: Any,
        connection_handler: SQLConnectionHandler,
        batch_size: int = 1000,
        **driver_kwargs: Any,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Yield PostgreSQL rows as dictionaries from a query."""
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

        if query:
            # Use custom query if provided
            def _execute_query():
                with connection_handler.lease() as conn:
                    result = conn.execute(text(query), row)
                    conn.commit()
                    return {"affected_rows": result.rowcount, "row": row}

        else:
            # Fall back to auto-generated INSERT query with PostgreSQL syntax
            columns = list(row.keys())
            columns_str = ", ".join(columns)
            placeholders = ", ".join([f":{col}" for col in columns])
            query = f"INSERT INTO {entity_name} ({columns_str}) VALUES ({placeholders})"

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

        if query:
            # Use custom query if provided
            def _execute_query():
                with connection_handler.lease() as conn:
                    # Execute custom query for each row
                    for _, row in frame.iterrows():
                        conn.execute(text(query), row.to_dict())
                    conn.commit()
                    return frame

        else:
            # Fall back to pandas.to_sql() with PostgreSQL optimizations
            def _execute_query():
                with connection_handler.lease() as conn:
                    frame.to_sql(
                        table,
                        conn,
                        if_exists="append",
                        index=False,
                        method="multi",
                        chunksize=1000,  # PostgreSQL-optimized chunk size
                    )
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
        """Write Dask DataFrame and return it."""
        query = driver_kwargs.get("query")
        table = driver_kwargs.get("table", entity_name)

        if query:
            # Use custom query if provided
            def _execute_query():
                with connection_handler.lease() as conn:
                    # Convert Dask DataFrame to pandas and execute custom query
                    pandas_df = frame.compute()
                    for _, row in pandas_df.iterrows():
                        conn.execute(text(query), row.to_dict())
                    conn.commit()
                    return frame

        else:
            # Fall back to pandas.to_sql() with PostgreSQL optimizations
            def _execute_query():
                with connection_handler.lease() as conn:
                    # Convert Dask DataFrame to pandas and write
                    pandas_df = frame.compute()
                    pandas_df.to_sql(
                        table,
                        conn,
                        if_exists="append",
                        index=False,
                        method="multi",
                        chunksize=1000,  # PostgreSQL-optimized chunk size
                    )
                    conn.commit()
                    return frame

        return await asyncio.to_thread(_execute_query)
