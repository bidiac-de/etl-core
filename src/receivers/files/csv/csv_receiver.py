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
    read_csv_row,
    read_csv_bulk,
    read_csv_bigdata,
    write_csv_row,
    write_csv_bulk,
    write_csv_bigdata,
)

_SENTINEL: Any = object()


def _next_or_sentinel(it):
    """Return next item from iterator or sentinel on StopIteration."""
    try:
        return next(it)
    except StopIteration:
        return _SENTINEL


class CSVReceiver(ReadFileReceiver, WriteFileReceiver):
    """Receiver for CSV files."""

    async def read_row(
        self,
        filepath: Path,
        metrics: ComponentMetrics,
        separator: str = ",",
    ) -> AsyncIterator[Dict[str, Any]]:
        """Stream CSV rows sequentially in a thread."""
        ensure_exists(filepath)
        it = read_csv_row(filepath, separator=separator)
        while True:
            row = await asyncio.to_thread(_next_or_sentinel, it)
            if row is _SENTINEL:
                break
            metrics.lines_received += 1
            yield row

    async def read_bulk(
        self, filepath: Path, metrics: ComponentMetrics
    ) -> pd.DataFrame:
        """Read entire CSV into a pandas DataFrame."""
        ensure_exists(filepath)
        df = await asyncio.to_thread(read_csv_bulk, filepath)
        metrics.lines_received += len(df)
        return df

    async def read_bigdata(
        self, filepath: Path, metrics: ComponentMetrics
    ) -> dd.DataFrame:
        """Read large CSV into a Dask DataFrame."""
        ensure_exists(filepath)
        ddf = await asyncio.to_thread(read_csv_bigdata, filepath)
        try:
            metrics.lines_received += int(ddf.map_partitions(len).sum().compute())
        except Exception:
            pass
        return ddf

    async def write_row(
        self, filepath: Path, metrics: ComponentMetrics, row: Dict[str, Any]
    ):
        """Append one row to a CSV file."""
        await asyncio.to_thread(write_csv_row, filepath, row)
        metrics.lines_received += 1

    async def write_bulk(
        self, filepath: Path, metrics: ComponentMetrics, data: List[Dict[str, Any]]
    ):
        """Write multiple rows to a CSV file."""
        await asyncio.to_thread(write_csv_bulk, filepath, data or [])
        metrics.lines_received += len(data) if data else 0

    async def write_bigdata(
        self, filepath: Path, metrics: ComponentMetrics, data: dd.DataFrame
    ):
        """Write large dataset to a CSV file."""
        await asyncio.to_thread(write_csv_bigdata, filepath, data)
