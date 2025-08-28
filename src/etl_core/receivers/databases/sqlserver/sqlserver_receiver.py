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
        metrics: ComponentMetrics,
        data: Any,
        params: Dict[str, Any],
        connection_handler: SQLConnectionHandler,
    ) -> Dict[str, Any]:
        """Write a single row to SQL Server."""
        with connection_handler.lease() as conn:
            # Build INSERT statement
            if isinstance(data, dict):
                columns = list(data.keys())
                values = list(data.values())
                placeholders = ", ".join([f":{col}" for col in columns])
                query = f"INSERT INTO {entity_name} ({', '.join(columns)}) VALUES ({placeholders})"
                
                result = conn.execute(text(query), data)
                conn.commit()
                return {"rows_affected": result.rowcount, "status": "success"}
            else:
                raise ValueError("Row data must be a dictionary")

    async def write_bulk(
        self,
        entity_name: str,
        metrics: ComponentMetrics,
        data: pd.DataFrame,
        params: Dict[str, Any],
        if_exists: str,
        connection_handler: SQLConnectionHandler,
    ) -> Dict[str, Any]:
        """Write pandas DataFrame to SQL Server."""
        with connection_handler.lease() as conn:
            # Use pandas to_sql for efficient bulk writing
            data.to_sql(
                name=entity_name,
                con=conn,
                if_exists=if_exists,
                index=False,
                method="multi",
                chunksize=1000
            )
            conn.commit()
            return {"rows_written": len(data), "status": "success"}

    async def write_bigdata(
        self,
        entity_name: str,
        metrics: ComponentMetrics,
        data: dd.DataFrame,
        params: Dict[str, Any],
        if_exists: str,
        connection_handler: SQLConnectionHandler,
    ) -> Dict[str, Any]:
        """Write Dask DataFrame to SQL Server in chunks."""
        total_rows = 0
        
        # Process partitions one by one to avoid memory issues
        for partition in data.map_partitions(lambda pdf: pdf).partitions:
            pdf = partition.compute()
            with connection_handler.lease() as conn:
                pdf.to_sql(
                    name=entity_name,
                    con=conn,
                    if_exists=if_exists if total_rows == 0 else "append",
                    index=False,
                    method="multi",
                    chunksize=1000
                )
                conn.commit()
                total_rows += len(pdf)
        
        return {"rows_written": total_rows, "status": "success"}
