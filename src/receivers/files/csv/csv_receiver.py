from pathlib import Path
from typing import Dict, Any, List, AsyncIterator, Union
import pandas as pd
import dask.dataframe as dd
import asyncio
from src.receivers.files.file_helper import ensure_exists
from src.receivers.files.read_file_receiver import ReadFileReceiver
from src.receivers.files.write_file_receiver import WriteFileReceiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.files.csv.csv_helper import (
    read_csv_row, read_csv_bulk, read_csv_bigdata,
    write_csv_row, write_csv_bulk, write_csv_bigdata,
)


class CSVReceiver(ReadFileReceiver, WriteFileReceiver):
    """Receiver for CSV files."""

    async def read_row(
            self, filepath: Path, metrics: ComponentMetrics, *, separator: str = ",",

    ) -> AsyncIterator[Dict[str, Any]]:
        """
        True streaming: pull one row at a time by stepping the sync generator
        on a worker thread. No full-file buffering.
        """
        it = read_csv_row(filepath, separator=separator)

        while True:
            try:
                row = await asyncio.to_thread(next, it)
            except StopIteration:
                break
            metrics.lines_received += 1
            yield row

    async def read_bulk(self, filepath: Path, metrics: ComponentMetrics) -> pd.DataFrame:
        """Read the entire CSV into a Pandas DataFrame."""
        ensure_exists(filepath)
        df = await asyncio.to_thread(read_csv_bulk, filepath)
        metrics.lines_received += len(df)
        return df

    async def read_bigdata(self, filepath: Path, metrics: ComponentMetrics) -> dd.DataFrame:
        """Read large CSV files as a Dask DataFrame."""
        ensure_exists(filepath)
        ddf = await asyncio.to_thread(read_csv_bigdata, filepath)
        try:
            metrics.lines_received += int(ddf.map_partitions(len).sum().compute())
        except Exception:
            pass
        return ddf

    async def write_row(self, filepath: Path, metrics: ComponentMetrics, row: Dict[str, Any]):
        """
        Append a single CSV record (header will be created if needed).
        Mirrors JSONReceiver: we also bump the metric here.
        """
        await asyncio.to_thread(write_csv_row, filepath, row)
        metrics.lines_received += 1

    async def write_bulk(self, filepath: Path, metrics: ComponentMetrics,
                         data: List[Dict[str, Any]]):
        """Write multiple CSV records."""
        await asyncio.to_thread(write_csv_bulk, filepath, data or [])
        metrics.lines_received += (len(data) if data else 0)

    async def write_bigdata(self, filepath: Path, metrics: ComponentMetrics, data: dd.DataFrame):
        """Write large datasets to the CSV file using Dask or a row generator."""
        await asyncio.to_thread(write_csv_bigdata, filepath, data)