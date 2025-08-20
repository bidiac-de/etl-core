from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Optional

import dask.dataframe as dd
import pandas as pd

from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.files.file_helper import ensure_exists
from src.receivers.files.read_file_receiver import ReadFileReceiver
from src.receivers.files.write_file_receiver import WriteFileReceiver
from src.receivers.files.file_helper import FileReceiverError
from src.receivers.files.excel.excel_helper import (
    read_excel_rows,
    read_excel_bulk,
    read_excel_bigdata,
    write_excel_row,
    write_excel_bulk,
    write_excel_bigdata,
)

_SENTINEL: Any = object()


def _next_or_sentinel(it) -> Any:
    try:
        return next(it)
    except StopIteration:
        return _SENTINEL


class ExcelReceiver(ReadFileReceiver, WriteFileReceiver):
    """Receiver for Excel files (xlsx/xlsm; xls supported)"""

    async def read_row(
        self,
        filepath: Path,
        metrics: ComponentMetrics,
        sheet_name: Optional[str] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Stream rows from an Excel sheet one by one."""
        ensure_exists(filepath)
        it = read_excel_rows(filepath, sheet_name=sheet_name)
        while True:
            row = await asyncio.to_thread(_next_or_sentinel, it)
            if row is _SENTINEL:
                break
            metrics.lines_received += 1
            yield row

    async def read_bulk(
        self,
        filepath: Path,
        metrics: ComponentMetrics,
        sheet_name: Optional[str] = None,
    ) -> pd.DataFrame:
        """Read an entire Excel sheet into a pandas DataFrame."""
        ensure_exists(filepath)
        df = await asyncio.to_thread(read_excel_bulk, filepath, sheet_name)
        metrics.lines_received += len(df)
        return df

    async def read_bigdata(
        self,
        filepath: Path,
        metrics: ComponentMetrics,
        sheet_name: Optional[str] = None,
    ) -> dd.DataFrame:
        """Load an Excel sheet as a Dask DataFrame."""
        ensure_exists(filepath)
        ddf = await asyncio.to_thread(read_excel_bigdata, filepath, sheet_name)
        try:
            count = await asyncio.to_thread(
                lambda: int(ddf.map_partitions(len).sum().compute())
            )
        except Exception:
            count = 0
        metrics.lines_received += count
        return ddf

    async def write_row(
        self,
        filepath: Path,
        metrics: ComponentMetrics,
        row: Dict[str, Any],
        sheet_name: Optional[str] = None,
    ) -> None:
        """Append a single row to an Excel sheet."""
        metrics.lines_received += 1
        try:
            await asyncio.to_thread(write_excel_row, filepath, row, sheet_name)
        except Exception as exc:
            raise FileReceiverError(f"Failed to write excel row: {exc}") from exc
        metrics.lines_forwarded += 1

    async def write_bulk(
        self,
        filepath: Path,
        metrics: ComponentMetrics,
        data: pd.DataFrame,
        sheet_name: Optional[str] = None,
    ) -> None:
        """Write a complete pandas DataFrame to an Excel sheet."""
        metrics.lines_received += len(data)
        try:
            await asyncio.to_thread(write_excel_bulk, filepath, data, sheet_name)
        except Exception as exc:
            raise FileReceiverError(f"Failed to write excel bulk: {exc}") from exc
        metrics.lines_forwarded += len(data)

    async def write_bigdata(
        self,
        filepath: Path,
        metrics: ComponentMetrics,
        data: dd.DataFrame,
        sheet_name: Optional[str] = None,
    ) -> None:
        """Write a Dask DataFrame to an Excel sheet (materialized to pandas)."""
        try:
            data = await asyncio.to_thread(data.persist)
        except Exception:
            pass

        try:
            row_count = await asyncio.to_thread(
                lambda: int(data.map_partitions(len).sum().compute())
            )
        except Exception as exc:
            raise FileReceiverError(
                f"Failed to count rows before excel write: {exc}"
            ) from exc

        try:
            await asyncio.to_thread(write_excel_bigdata, filepath, data, sheet_name)
        except Exception as exc:
            raise FileReceiverError(f"Failed to write excel: {exc}") from exc

        metrics.lines_forwarded += row_count
