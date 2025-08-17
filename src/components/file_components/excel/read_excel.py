from __future__ import annotations

from typing import Any, AsyncIterator, Dict, Optional

import dask.dataframe as dd
import pandas as pd
from pydantic import model_validator

from src.components.file_components.excel.excel_component import Excel
from src.components.component_registry  import register_component
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.files.excel.excel_receiver import ExcelReceiver


@register_component("read_excel")
class ReadExcel(Excel):
    """Excel reader supporting row, bulk and bigdata modes."""

    @model_validator(mode="after")
    def _build_objects(self) -> "ReadExcel":
        self._receiver = ExcelReceiver()
        return self

    async def process_row(
            self,
            row: Dict[str, Any],
            metrics: ComponentMetrics,
            sheet_name: Optional[str] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Stream rows as dicts from the sheet."""
        _ = row
        async for out in self._receiver.read_row(
                self.filepath, metrics=metrics, sheet_name=sheet_name or self.sheet_name
        ):
            yield out

    async def process_bulk(
            self,
            data: Optional[pd.DataFrame],
            metrics: ComponentMetrics,
            sheet_name: Optional[str] = None,
    ) -> AsyncIterator[pd.DataFrame]:
        """Read the entire sheet into a pandas DataFrame."""
        _ = data
        df = await self._receiver.read_bulk(
            self.filepath, metrics=metrics, sheet_name=sheet_name or self.sheet_name
        )
        yield df

    async def process_bigdata(
            self,
            chunk_iterable: Any,
            metrics: ComponentMetrics,
            sheet_name: Optional[str] = None,
    ) -> AsyncIterator[dd.DataFrame]:
        """Read the sheet as a Dask DataFrame."""
        _ = chunk_iterable
        ddf = await self._receiver.read_bigdata(
            self.filepath, metrics=metrics, sheet_name=sheet_name or self.sheet_name
        )
        yield ddf