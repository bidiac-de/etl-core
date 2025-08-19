import asyncio
from typing import Dict, Any, List, AsyncIterator, Union
import pandas as pd
import dask.dataframe as dd
from sqlalchemy import text
from sqlalchemy.engine import Connection as SQLConnection

from src.etl_core.receivers.databases.read_database_receiver import (
    ReadDatabaseReceiver,
)
from src.etl_core.receivers.databases.write_database_receiver import (
    WriteDatabaseReceiver,
)
from src.etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from src.etl_core.components.databases.sql_connection_handler import (
    SQLConnectionHandler,
)


class MariaDBReceiver(ReadDatabaseReceiver, WriteDatabaseReceiver):
    """Receiver for MariaDB operations supporting row, bulk, and bigdata modes."""

    def __init__(self, connection_handler: SQLConnectionHandler):
        # Use object.__setattr__ to avoid Pydantic validation issues
        object.__setattr__(self, "_connection_handler", connection_handler)

    @property
    def connection_handler(self) -> SQLConnectionHandler:
        return self._connection_handler

    def _get_connection(self) -> SQLConnection:
        """Get the SQLAlchemy connection from the connection handler."""
        # Use the lease context manager to get a connection
        return self.connection_handler.lease().__enter__()

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
        # Use entity_name as table name if no specific query provided
        query = driver_kwargs.get('query', f"SELECT * FROM {entity_name}")
        params = driver_kwargs.get('params', {})
        
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
        """Read query results into a Pandas DataFrame."""
        query = driver_kwargs.get('query', f"SELECT * FROM {entity_name}")
        params = driver_kwargs.get('params', {})

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
        chunk_size: int = 100_000,
        **driver_kwargs: Any,
    ) -> dd.DataFrame:
        """Read large query results as a Dask DataFrame."""
        query = driver_kwargs.get('query', f"SELECT * FROM {entity_name}")
        params = driver_kwargs.get('params', {})

        def _execute_query():
            with self.connection_handler.lease() as conn:
                result = conn.execute(text(query), params)
                df = pd.DataFrame([dict(row._mapping) for row in result])
                return dd.from_pandas(df, npartitions=4)  # Default to 4 partitions

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
        """Write a single row to a MariaDB table."""
        table = driver_kwargs.get('table', entity_name)

        def _execute_insert():
            with self.connection_handler.lease() as conn:
                columns = ", ".join(row.keys())
                placeholders = ", ".join([f":{key}" for key in row.keys()])
                query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                result = conn.execute(text(query), row)
                conn.commit()
                return {"inserted_id": result.inserted_primary_key[0] if result.inserted_primary_key else None}

        return await asyncio.to_thread(_execute_insert)

    async def write_bulk(
        self,
        *,
        db: Any,
        entity_name: str,
        frame: pd.DataFrame,
        metrics: Any,
        chunk_size: int = 50_000,
        **driver_kwargs: Any,
    ) -> pd.DataFrame:
        """Write multiple rows to a MariaDB table."""
        table = driver_kwargs.get('table', entity_name)

        def _execute_bulk_insert():
            with self.connection_handler.lease() as conn:
                rows = frame.to_dict("records")
                
                if not rows:
                    return frame

                # Get columns from first row
                columns = list(rows[0].keys())
                placeholders = ", ".join([f":{key}" for key in columns])
                query = (
                    f"INSERT INTO {table} ({', '.join(columns)}) "
                    f"VALUES ({placeholders})"
                )

                # Execute batch insert
                conn.execute(text(query), rows)
                conn.commit()
                return frame

        return await asyncio.to_thread(_execute_bulk_insert)

    async def write_bigdata(
        self,
        *,
        db: Any,
        entity_name: str,
        frame: "dd.DataFrame",
        metrics: Any,
        partition_chunk_size: int = 50_000,
        **driver_kwargs: Any,
    ) -> "dd.DataFrame":
        """Write Dask DataFrame to MariaDB table by processing partitions."""
        table = driver_kwargs.get('table', entity_name)

        def _process_partition(partition_df):
            with self.connection_handler.lease() as conn:
                rows = partition_df.to_dict("records")

                if not rows:
                    return partition_df

                columns = list(rows[0].keys())
                placeholders = ", ".join([f":{key}" for key in columns])
                query = (
                    f"INSERT INTO {table} ({', '.join(columns)}) "
                    f"VALUES ({placeholders})"
                )

                conn.execute(text(query), rows)
                conn.commit()
                return partition_df

        # Process each partition
        result = frame.map_partitions(_process_partition).compute()
        # The compute() ensures all partitions are processed
        return result
