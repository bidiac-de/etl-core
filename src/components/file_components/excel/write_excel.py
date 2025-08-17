from __future__ import annotations

from typing import Any, AsyncIterator, Dict, List, Optional, Union

import dask.dataframe as dd
import pandas as pd
from pydantic import model_validator

from src.components.file_components.excel.excel_component import Excel
from src.components.component_registry  import register_component
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.files.excel.excel_receiver import ExcelReceiver


@register_component("write_excel")
class WriteExcel(Excel):
    """Excel writer supporting row, bulk and bigdata modes."""

    @model_validator(mode="after")
    def _build_objects(self) -> "WriteExcel":
        self._receiver = ExcelReceiver()
        return self

    async def process_row(
            self,
            row: Dict[str, Any],
            metrics: ComponentMetrics,
            sheet_name: Optional[str] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Write one row and yield it back."""
        await self._receiver.write_row(
            self.filepath,
            metrics=metrics,
            row=row,
            sheet_name=sheet_name or self.sheet_name,
        )
        yield row

    async def process_bulk(
            self,
            data: Union[pd.DataFrame, List[Dict[str, Any]]],
            metrics: ComponentMetrics,
            sheet_name: Optional[str] = None,
    ) -> AsyncIterator[pd.DataFrame]:
        """Write multiple rows or DataFrame, yield as DataFrame."""
        await self._receiver.write_bulk(
            self.filepath,
            metrics=metrics,
            data=data,
            sheet_name=sheet_name or self.sheet_name,
        )
        yield data if isinstance(data, pd.DataFrame) else pd.DataFrame(data or [])

    async def process_bigdata(
            self,
            chunk_iterable: Any,
            metrics: ComponentMetrics,
            sheet_name: Optional[str] = None,
    ) -> AsyncIterator[dd.DataFrame]:
        """Write Dask DataFrame (or iterable converted to Dask) and yield it."""
        if not isinstance(chunk_iterable, dd.DataFrame):
            pdf = pd.DataFrame(list(chunk_iterable))
            ddf = dd.from_pandas(pdf, npartitions=max(1, min(8, len(pdf) or 1)))
        else:
            ddf = chunk_iterable

        await self._receiver.write_bigdata(
            self.filepath,
            metrics=metrics,
            data=ddf,
            sheet_name=sheet_name or self.sheet_name,
        )
        yield ddf