from __future__ import annotations

from pathlib import Path
from typing import Any, AsyncIterator, Dict, Iterable, Optional

import dask.dataframe as dd
import pandas as pd

from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.files.excel.excel_helper import ExcelHelper


class ExcelReceiver:
    """
    Stateless receiver for Excel I/O.
    """

    async def read_row(
            self,
            *,
            filepath: Path,
            metrics: ComponentMetrics,
            sheet_name: Optional[str | int] = 0,
            start_index: int = 0,
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Yield one row (dict) at a time from the given Excel file/sheet.
        """
        _ = metrics
        df = ExcelHelper.read_excel(filepath, sheet_name=sheet_name)
        start = max(0, int(start_index))
        for _, row in df.iloc[start:].iterrows():
            yield row.to_dict()

    async def read_bulk(
            self,
            *,
            filepath: Path,
            metrics: ComponentMetrics,
            sheet_name: Optional[str | int] = 0,
    ) -> AsyncIterator[pd.DataFrame]:
        """
        Yield a single pandas DataFrame from the given Excel file/sheet.
        """
        _ = metrics
        df = ExcelHelper.read_excel(filepath, sheet_name=sheet_name)
        yield df

    async def read_bigdata(
            self,
            *,
            filepath: Path,
            metrics: ComponentMetrics,
            sheet_name: Optional[str | int] = 0,
            npartitions: Optional[int] = None,
    ) -> AsyncIterator[dd.DataFrame]:
        """
        Yield a Dask DataFrame built from the Excel content.

        Implementation detail:
        - Load via pandas and partition with dask.from_pandas.
        """
        _ = metrics
        pdf = ExcelHelper.read_excel(filepath, sheet_name=sheet_name)

        if npartitions is None:
            rows = max(1, len(pdf))
            npartitions = max(1, rows // 100_000)

        ddf = dd.from_pandas(pdf, npartitions=npartitions)
        yield ddf


    async def write_row(
            self,
            *,
            filepath: Path,
            metrics: ComponentMetrics,
            row: Dict[str, Any],
            sheet_name: Optional[str] = "Sheet1",
            append: bool = True,
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Write a single row (append by default for .xlsx/.xlsm) and yield it back.

        For .xls, append falls back to overwrite due to engine limitations.
        """
        _ = metrics
        df = pd.DataFrame([row])
        ExcelHelper.write_excel(
            df,
            filepath,
            sheet_name=sheet_name,
            append=append,
        )
        yield row

    async def write_bulk(
            self,
            *,
            filepath: Path,
            metrics: ComponentMetrics,
            data: pd.DataFrame | Iterable[Dict[str, Any]] | None,
            sheet_name: Optional[str] = "Sheet1",
            append: bool = False,
    ) -> AsyncIterator[pd.DataFrame]:
        """
        Write a DataFrame or iterable of dicts and yield a DataFrame once.

        - If 'data' is an iterable of dicts, it is normalized to a DataFrame.
        - 'append=True' only applies to .xlsx/.xlsm; .xls will be overwritten.
        """
        _ = metrics
        if isinstance(data, pd.DataFrame):
            df = data
        else:
            recs = list(data or [])
            df = pd.DataFrame(recs)

        ExcelHelper.write_excel(
            df,
            filepath,
            sheet_name=sheet_name,
            append=append,
        )
        yield df

    async def write_bigdata(
            self,
            *,
            filepath: Path,
            metrics: ComponentMetrics,
            data: dd.DataFrame,
            sheet_name: Optional[str] = "Sheet1",
    ) -> AsyncIterator[dd.DataFrame]:
        """
        Write a Dask DataFrame to a single Excel sheet and yield it once.

        Note:
        - Excel is not ideal for partitioned data; we materialize to pandas.
        """
        _ = metrics
        pdf = data.compute()
        ExcelHelper.write_excel(pdf, filepath, sheet_name=sheet_name, append=False)
        yield data