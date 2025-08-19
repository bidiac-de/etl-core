import asyncio
from typing import Dict, Any, List, AsyncIterator, Union
import pandas as pd
import dask.dataframe as dd
from sqlalchemy import text
from sqlalchemy.engine import Connection as SQLConnection

from etl_core.receivers.databases.mariadb.read_database_receiver import (
    ReadDatabaseReceiver,
)
from etl_core.receivers.databases.mariadb.write_database_receiver import (
    WriteDatabaseReceiver,
)
from src.etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from src.etl_core.components.databases.sql_connection_handler import (
    SQLConnectionHandler,
)


class MariaDBReceiver(ReadDatabaseReceiver, WriteDatabaseReceiver):
    """Receiver for MariaDB operations supporting row, bulk, and bigdata modes."""

    def __init__(self, connection_handler: SQLConnectionHandler):
        # Initialize both parent classes properly
        ReadDatabaseReceiver.__init__(self, connection_handler)
        WriteDatabaseReceiver.__init__(self, connection_handler)

    def _get_connection(self) -> SQLConnection:
        """Get the SQLAlchemy connection from the connection handler."""
        # Use the lease context manager to get a connection
        return self.connection_handler.lease().__enter__()

    async def read_row(
        self, query: str, params: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncIterator[Dict[str, Any]]:
        """Yield MariaDB rows as dictionaries."""

        def _execute_query():
            with self.connection_handler.lease() as conn:
                result = conn.execute(text(query), params)
                return [dict(row._mapping) for row in result]

        rows = await asyncio.to_thread(_execute_query)
        for row in rows:
            yield row

    async def read_bulk(
        self, query: str, params: Dict[str, Any], metrics: ComponentMetrics
    ) -> pd.DataFrame:
        """Read query results into a Pandas DataFrame."""

        def _execute_query():
            with self.connection_handler.lease() as conn:
                result = conn.execute(text(query), params)
                return pd.DataFrame([dict(row._mapping) for row in result])

        return await asyncio.to_thread(_execute_query)

    async def read_bigdata(
        self, query: str, params: Dict[str, Any], metrics: ComponentMetrics
    ) -> dd.DataFrame:
        """Read large query results as a Dask DataFrame."""

        def _execute_query():
            with self.connection_handler.lease() as conn:
                result = conn.execute(text(query), params)
                df = pd.DataFrame([dict(row._mapping) for row in result])
                return dd.from_pandas(df, npartitions=4)  # Default to 4 partitions

        return await asyncio.to_thread(_execute_query)

    async def write_row(
        self, table: str, data: Dict[str, Any], metrics: ComponentMetrics
    ) -> None:
        """Write a single row to a MariaDB table."""

        def _execute_insert():
            with self.connection_handler.lease() as conn:
                columns = ", ".join(data.keys())
                placeholders = ", ".join([f":{key}" for key in data.keys()])
                query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                conn.execute(text(query), data)
                conn.commit()

        await asyncio.to_thread(_execute_insert)

    async def write_bulk(
        self,
        table: str,
        data: Union[pd.DataFrame, List[Dict[str, Any]]],
        metrics: ComponentMetrics,
    ) -> None:
        """Write multiple rows to a MariaDB table."""

        def _execute_bulk_insert():
            with self.connection_handler.lease() as conn:

                if isinstance(data, pd.DataFrame):
                    # Convert DataFrame to list of dicts
                    rows = data.to_dict("records")
                else:
                    rows = data

                if not rows:
                    return

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

        await asyncio.to_thread(_execute_bulk_insert)

    async def write_bigdata(
        self, table: str, metrics: ComponentMetrics, data: dd.DataFrame
    ) -> None:
        """Write Dask DataFrame to MariaDB table by processing partitions."""

        def _process_partition(partition_df):
            with self.connection_handler.lease() as conn:
                rows = partition_df.to_dict("records")

                if not rows:
                    return

                columns = list(rows[0].keys())
                placeholders = ", ".join([f":{key}" for key in columns])
                query = (
                    f"INSERT INTO {table} ({', '.join(columns)}) "
                    f"VALUES ({placeholders})"
                )

                conn.execute(text(query), rows)
                conn.commit()

        # Process each partition
        data.map_partitions(_process_partition).compute()
        # The compute() ensures all partitions are processed
