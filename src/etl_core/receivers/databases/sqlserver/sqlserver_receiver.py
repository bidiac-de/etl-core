from __future__ import annotations

from typing import Any, AsyncIterator, Dict

import dask.dataframe as dd
import pandas as pd
from sqlalchemy import text

from etl_core.components.databases.sql_connection_handler import SQLConnectionHandler
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics


class SQLServerReceiver:
    """SQL Server receiver for database operations."""

    async def read_row(
        self,
        entity_name: str,
        metrics: ComponentMetrics,
        query: str,
        params: Dict[str, Any],
        connection_handler: SQLConnectionHandler,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Read rows one-by-one from SQL Server."""
        with connection_handler.lease() as conn:
            # Use the provided query or build a default one
            if not query:
                query = f"SELECT * FROM {entity_name}"
            
            result = conn.execute(text(query), params)
            
            for row in result:
                # Convert row to dict for easier handling
                row_dict = dict(row._mapping)
                yield row_dict

    async def read_bulk(
        self,
        entity_name: str,
        metrics: ComponentMetrics,
        query: str,
        params: Dict[str, Any],
        connection_handler: SQLConnectionHandler,
    ) -> pd.DataFrame:
        """Read full result as a pandas DataFrame from SQL Server."""
        with connection_handler.lease() as conn:
            # Use the provided query or build a default one
            if not query:
                query = f"SELECT * FROM {entity_name}"
            
            # Use pandas read_sql for efficient DataFrame creation
            df = pd.read_sql(query, conn, params=params)
            return df

    async def read_bigdata(
        self,
        entity_name: str,
        metrics: ComponentMetrics,
        query: str,
        params: Dict[str, Any],
        connection_handler: SQLConnectionHandler,
    ) -> dd.DataFrame:
        """Read result as a Dask DataFrame from SQL Server."""
        with connection_handler.lease() as conn:
            # Use the provided query or build a default one
            if not query:
                query = f"SELECT * FROM {entity_name}"
            
            # First read as pandas, then convert to Dask
            df = pd.read_sql(query, conn, params=params)
            ddf = dd.from_pandas(df, npartitions=1)
            return ddf

    async def write_row(
        self,
        entity_name: str,
        row: Dict[str, Any],
        metrics: ComponentMetrics,
        table: str,
        query: str = "",
        connection_handler: SQLConnectionHandler = None,
        **driver_kwargs: Any,
    ) -> Dict[str, Any]:
        """Write a single row to SQL Server."""
        with connection_handler.lease() as conn:
            if query:
                # Execute custom query
                result = conn.execute(text(query), row)
            else:
                # Build INSERT statement
                columns = list(row.keys())
                placeholders = ", ".join([f":{col}" for col in columns])
                query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
                
                result = conn.execute(text(query), row)
            
            conn.commit()
            return {"affected_rows": result.rowcount, "row": row}

    async def write_bulk(
        self,
        entity_name: str,
        frame: pd.DataFrame,
        metrics: ComponentMetrics,
        table: str,
        query: str = "",
        if_exists: str = "append",
        bulk_chunk_size: int = 50_000,
        connection_handler: SQLConnectionHandler = None,
        **driver_kwargs: Any,
    ) -> pd.DataFrame:
        """Write pandas DataFrame to SQL Server."""
        if frame.empty:
            return frame

        def _execute_query(conn, df):
            if query:
                # Execute custom query for each row
                for _, row in df.iterrows():
                    conn.execute(text(query), row.to_dict())
            else:
                # Use pandas to_sql for efficient bulk writing
                df.to_sql(
                    name=table,
                    con=conn,
                    if_exists=if_exists,
                    index=False,
                    method="multi",
                    chunksize=bulk_chunk_size
                )

        with connection_handler.lease() as conn:
            _execute_query(conn, frame)
            conn.commit()
        
        return frame

    async def write_bigdata(
        self,
        entity_name: str,
        frame: dd.DataFrame,
        metrics: ComponentMetrics,
        table: str,
        query: str = "",
        if_exists: str = "append",
        bigdata_partition_chunk_size: int = 50_000,
        connection_handler: SQLConnectionHandler = None,
        **driver_kwargs: Any,
    ) -> dd.DataFrame:
        """Write Dask DataFrame to SQL Server in chunks."""
        def _execute_query(conn, df):
            if query:
                # Execute custom query for each row
                for _, row in df.iterrows():
                    conn.execute(text(query), row.to_dict())
            else:
                # Use pandas to_sql for efficient bulk writing
                df.to_sql(
                    name=table,
                    con=conn,
                    if_exists=if_exists,
                    index=False,
                    method="multi",
                    chunksize=bigdata_partition_chunk_size
                )

        # Process partitions one by one to avoid memory issues
        for partition in frame.map_partitions(lambda pdf: pdf).partitions:
            pdf = partition.compute()
            
            if not pdf.empty:
                with connection_handler.lease() as conn:
                    _execute_query(conn, pdf)
                    conn.commit()
        
        return frame
