import asyncio
from typing import Any, Dict, AsyncIterator

import pandas as pd
import dask.dataframe as dd
from sqlalchemy import text

from etl_core.components.databases.sql_connection_handler import SQLConnectionHandler
from src.etl_core.receivers.databases.sql_receiver import SQLReceiver


class MariaDBReceiver(SQLReceiver):
    """Receiver for MariaDB operations supporting row, bulk, and bigdata modes."""

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

        if query:
            # Use custom query if provided
            def _execute_query():
                with connection_handler.lease() as conn:
                    result = conn.execute(text(query), row)
                    conn.commit()
                    return {"affected_rows": result.rowcount, "row": row}

        else:
            # Fall back to auto-generated INSERT query
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
            # Fall back to pandas.to_sql()
            def _execute_query():
                with connection_handler.lease() as conn:
                    frame.to_sql(
                        table,
                        conn,
                        if_exists="append",
                        index=False,
                        method="multi",
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
        """Write a Dask DataFrame and return it."""
        # Don't check frame.empty for Dask DataFrames as it's expensive
        # Empty partitions will be handled within the processing logic

        query = driver_kwargs.get("query")
        table = driver_kwargs.get("table", entity_name)
        if_exists = driver_kwargs.get("if_exists", "append")
        chunk_size = driver_kwargs.get("bigdata_partition_chunk_size", 50_000)

        def _execute_query():
            with connection_handler.lease() as conn:
                if query:
                    # Use custom query if provided
                    for partition in frame.map_partitions(lambda pdf: pdf).partitions:
                        pdf = partition.compute()
                        if not pdf.empty:  # Check individual partition instead
                            for _, row in pdf.iterrows():
                                conn.execute(text(query), row.to_dict())
                else:
                    # Fall back to pandas.to_sql() for each partition
                    total_rows = 0
                    for partition in frame.map_partitions(lambda pdf: pdf).partitions:
                        pdf = partition.compute()
                        if not pdf.empty:  # Check individual partition instead
                            pdf.to_sql(
                                table,
                                conn,
                                if_exists=if_exists if total_rows == 0 else "append",
                                index=False,
                                method="multi",
                                chunksize=chunk_size,
                            )
                            total_rows += len(pdf)
                conn.commit()
                return frame

        return await asyncio.to_thread(_execute_query)
