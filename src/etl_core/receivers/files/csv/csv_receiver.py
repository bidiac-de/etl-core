from pathlib import Path
from typing import Dict, Any, AsyncIterator
import pandas as pd
import dask.dataframe as dd
import asyncio

from etl_core.receivers.files.file_helper import ensure_file_exists, FileReceiverError
from etl_core.receivers.files.read_file_receiver import ReadFileReceiver
from etl_core.receivers.files.write_file_receiver import WriteFileReceiver
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.files.csv.csv_helper import (
    read_csv_row,
    read_csv_bulk,
    read_csv_bigdata,
    write_csv_row,
    write_csv_bulk,
    write_csv_bigdata,
)

_SENTINEL: Any = object()


class CSVReceiver(ReadFileReceiver, WriteFileReceiver):
    """Receiver for CSV files."""

    async def read_row(
        self,
        filepath: Path,
        metrics: ComponentMetrics,
        separator: str,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Stream CSV rows sequentially in a thread."""
        ensure_file_exists(filepath)
        it = read_csv_row(filepath, separator=separator)
        while True:
            row = await asyncio.to_thread(next, it, _SENTINEL)
            if row is _SENTINEL:
                break
            metrics.lines_received += 1
            yield row

    async def read_bulk(
        self, filepath: Path, metrics: ComponentMetrics, separator: str
    ) -> pd.DataFrame:
        """Read entire CSV into a pandas DataFrame."""
        ensure_file_exists(filepath)
        df = await asyncio.to_thread(read_csv_bulk, filepath, separator=separator)
        metrics.lines_received += len(df)
        return df

    async def read_bigdata(
        self, filepath: Path, metrics: ComponentMetrics, separator: str
    ) -> dd.DataFrame:
        """Read large CSV into a Dask DataFrame."""
        ensure_file_exists(filepath)
        ddf = await asyncio.to_thread(read_csv_bigdata, filepath, separator=separator)
        metrics.lines_received += int(ddf.map_partitions(len).sum().compute())
        return ddf

    async def write_row(
        self,
        filepath: Path,
        metrics: ComponentMetrics,
        row: Dict[str, Any],
        separator: str,
    ):
        """Append one row to a CSV file."""
        metrics.lines_received += 1
        await asyncio.to_thread(write_csv_row, filepath, row, separator=separator)
        metrics.lines_forwarded += 1

    async def write_bulk(
        self,
        filepath: Path,
        metrics: ComponentMetrics,
        data: pd.DataFrame,
        separator: str,
    ):
        """Write multiple rows to a CSV file."""
        metrics.lines_received += len(data)
        await asyncio.to_thread(write_csv_bulk, filepath, data, separator=separator)
        metrics.lines_forwarded += len(data)

    async def write_bigdata(
        self,
        filepath: Path,
        metrics: ComponentMetrics,
        data: dd.DataFrame,
        separator: str,
    ):
        """Write large dataset to a CSV file."""
        try:
            data = await asyncio.to_thread(data.persist)
        except Exception as e:
            raise FileReceiverError(
                f"Failed to persist dataframe before write: {e}"
            ) from e

        try:
            row_count = await asyncio.to_thread(
                lambda: int(data.map_partitions(len).sum().compute())
            )
        except Exception as e:
            raise FileReceiverError(f"Failed to count rows; aborting write: {e}") from e

        try:
            await asyncio.to_thread(
                write_csv_bigdata, filepath, data, separator=separator
            )
        except Exception as e:
            raise FileReceiverError(f"Failed to write CSV: {e}") from e

        metrics.lines_forwarded += row_count
