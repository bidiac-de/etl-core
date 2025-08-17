from __future__ import annotations

from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Union

import asyncio
import dask.dataframe as dd
import pandas as pd

from src.receivers.base_receiver import Receiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.files.file_helper import ensure_exists
from src.receivers.files.excel.excel_helper import (
    read_excel_rows,
    read_excel_bulk,
    read_excel_bigdata,
    write_excel_row,
    write_excel_bulk,
    write_excel_bigdata,
)

_SENTINEL: Any = object()


def _next_or_sentinel(it):
    """Return next item from iterator or sentinel."""
    try:
        return next(it)
    except StopIteration:
        return _SENTINEL


class ExcelReceiver(Receiver):
    """Stateless receiver for Excel I/O."""

    async def read_row(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            sheet_name: Optional[str] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Stream rows from an Excel sheet as dicts."""
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
        """Read entire Excel sheet into DataFrame."""
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
        """Read Excel sheet as Dask DataFrame."""
        ensure_exists(filepath)
        ddf = await asyncio.to_thread(read_excel_bigdata, filepath, sheet_name)
        try:
            metrics.lines_received += int(ddf.map_partitions(len).sum().compute())
        except Exception:
            pass
        return ddf

    async def write_row(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            row: Dict[str, Any],
            sheet_name: Optional[str] = None,
    ) -> None:
        """Append one row to Excel file."""
        await asyncio.to_thread(write_excel_row, filepath, row, sheet_name)
        metrics.lines_received += 1

    async def write_bulk(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            data: Union[pd.DataFrame, List[Dict[str, Any]]],
            sheet_name: Optional[str] = None,
    ) -> None:
        """Write multiple rows/DataFrame to Excel file."""
        await asyncio.to_thread(write_excel_bulk, filepath, data, sheet_name)
        if isinstance(data, pd.DataFrame):
            metrics.lines_received += len(data)
        else:
            metrics.lines_received += len(data) if data else 0

    async def write_bigdata(
            self,
            filepath: Path,
            metrics: ComponentMetrics,
            data: dd.DataFrame,
            sheet_name: Optional[str] = None,
    ) -> None:
        """Write Dask DataFrame to Excel file."""
        await asyncio.to_thread(write_excel_bigdata, filepath, data, sheet_name)
        try:
            metrics.lines_received += int(data.shape[0].compute())
        except Exception:
            try:
                metrics.lines_received += int(data.index.size.compute())
            except Exception:
                import pandas as _pd

                metrics.lines_received += int(
                    data.map_partitions(
                        lambda df: _pd.Series([len(df)])
                    ).compute().sum()
                )