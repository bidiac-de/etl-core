# src/receivers/files/csv_receiver.py
from pathlib import Path
from typing import Dict, Any, List, AsyncIterator, Union
import pandas as pd
import dask.dataframe as dd
import asyncio

from src.receivers.read_file_receiver import ReadFileReceiver
from src.receivers.write_file_receiver import WriteFileReceiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.files.csv_helper import (
    read_csv_row, read_csv_bulk, read_csv_bigdata,
    write_csv_row, write_csv_bulk, write_csv_bigdata,
)

class CSVReceiver(ReadFileReceiver, WriteFileReceiver):
    """Receiver for CSV files with Pandas (bulk) and Dask (bigdata)."""

    async def read_row(self, filepath: Path, metrics: ComponentMetrics) -> AsyncIterator[Dict[str, Any]]:
        """Yield CSV rows as dictionaries."""
        # Sync → Async Brücke: im Thread lesen, danach yielden
        rows = await asyncio.to_thread(lambda: list(read_csv_row(filepath)))
        for row in rows:
            yield row

    async def read_bulk(self, filepath: Path, metrics: ComponentMetrics) -> pd.DataFrame:
        """Read the entire CSV into a Pandas DataFrame."""
        return await asyncio.to_thread(read_csv_bulk, filepath)

    async def read_bigdata(self, filepath: Path, metrics: ComponentMetrics) -> dd.DataFrame:
        """Read large CSV files efficiently using Dask."""
        return await asyncio.to_thread(read_csv_bigdata, filepath)

    async def write_row(self, filepath: Path, metrics: ComponentMetrics, row: Dict[str, Any]):
        """Write a single row to the CSV file."""
        await asyncio.to_thread(write_csv_row, filepath, row)

    async def write_bulk(self, filepath: Path, metrics: ComponentMetrics, data: List[Dict[str, Any]]):
        """Write multiple rows to the CSV file."""
        if data:
            await asyncio.to_thread(write_csv_bulk, filepath, data)

    async def write_bigdata(self, filepath: Path, metrics: ComponentMetrics, data: dd.DataFrame):
        """Write large datasets to the CSV file using Dask or a row generator."""
        await asyncio.to_thread(write_csv_bigdata, filepath, data)