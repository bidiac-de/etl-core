import asyncio
from typing import Any, Dict, AsyncIterator

import pandas as pd
import dask.dataframe as dd
from sqlalchemy import text

from src.etl_core.receivers.databases.sql_receiver import SQLReceiver


class MariaDBReceiver(SQLReceiver):
    """Receiver for MariaDB operations supporting row, bulk, and bigdata modes."""

    async def read_row(
        self,
        *,
        db: Any,
        entity_name: str,
        metrics: Any,
        batch_size: int = 1000,
        **driver_kwargs: Any,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Yield MariaDB rows as dictionaries from a query."""
        query = driver_kwargs.get("query", f"SELECT * FROM {entity_name}")
        params = driver_kwargs.get("params", {})

        def _execute_query():
            with self.connection_handler.lease() as conn:
                result = conn.execute(text(query), params)
                return [dict(row._mapping) for row in result]

        rows = await asyncio.to_thread(_execute_query)
        for row in rows:
            yield row

    async def read_bulk(
        self,
        *,
        db: Any,
        entity_name: str,
        metrics: Any,
        **driver_kwargs: Any,
    ) -> pd.DataFrame:
        """Read query results as a pandas DataFrame."""
        query = driver_kwargs.get("query", f"SELECT * FROM {entity_name}")
        params = driver_kwargs.get("params", {})

        def _execute_query():
            with self.connection_handler.lease() as conn:
                result = conn.execute(text(query), params)
                return pd.DataFrame([dict(row._mapping) for row in result])

        return await asyncio.to_thread(_execute_query)

    async def read_bigdata(
        self,
        *,
        db: Any,
        entity_name: str,
        metrics: Any,
        **driver_kwargs: Any,
    ) -> dd.DataFrame:
        """Read large query results as a Dask DataFrame."""
        query = driver_kwargs.get("query", f"SELECT * FROM {entity_name}")
        params = driver_kwargs.get("params", {})

        def _execute_query():
            with self.connection_handler.lease() as conn:
                result = conn.execute(text(query), params)
                df = pd.DataFrame([dict(row._mapping) for row in result])
                return dd.from_pandas(df, npartitions=1)

        return await asyncio.to_thread(_execute_query)

    async def write_row(
        self,
        *,
        db: Any,
        entity_name: str,
        row: Dict[str, Any],
        metrics: Any,
        **driver_kwargs: Any,
    ) -> Dict[str, Any]:
        """Write a single row and return the result."""
        columns = list(row.keys())
        columns_str = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])
        query = f"INSERT INTO {entity_name} ({columns_str}) VALUES ({placeholders})"

        def _execute_query():
            with self.connection_handler.lease() as conn:
                result = conn.execute(text(query), row)
                conn.commit()
                return {"affected_rows": result.rowcount, "row": row}

        return await asyncio.to_thread(_execute_query)

    async def write_bulk(
        self,
        *,
        db: Any,
        entity_name: str,
        frame: pd.DataFrame,
        metrics: Any,
        **driver_kwargs: Any,
    ) -> pd.DataFrame:
        """Write a pandas DataFrame and return the result."""
        if frame.empty:
            return frame

        columns = list(frame.columns)
        columns_str = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])
        query = f"INSERT INTO {entity_name} ({columns_str}) VALUES ({placeholders})"

        def _execute_query():
            with self.connection_handler.lease() as conn:
                data = frame.to_dict("records")
                conn.execute(text(query), data)
                conn.commit()
                return frame

        return await asyncio.to_thread(_execute_query)

    async def write_bigdata(
        self,
        *,
        db: Any,
        entity_name: str,
        frame: dd.DataFrame,
        metrics: Any,
        **driver_kwargs: Any,
    ) -> dd.DataFrame:
        """Write a Dask DataFrame and return the result."""

        def _write_chunk(chunk_df):
            import asyncio

            loop = asyncio.get_event_loop()
            return loop.run_until_complete(
                self.write_bulk(
                    db=db,
                    entity_name=entity_name,
                    frame=chunk_df,
                    metrics=metrics,
                    **driver_kwargs,
                )
            )

        try:
            frame.map_partitions(_write_chunk).compute()
        except Exception:
            pass

        return frame
