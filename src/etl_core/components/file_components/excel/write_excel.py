from __future__ import annotations

from typing import Any, Dict, AsyncGenerator

import pandas as pd
import dask.dataframe as dd
from pydantic import model_validator

from etl_core.components.file_components.excel.excel_component import Excel
from src.etl_core.components.component_registry import register_component
from src.etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.files.excel.excel_receiver import ExcelReceiver


@register_component("write_excel")
class WriteExcel(Excel):
    """Excel writer supporting row, bulk, and bigdata modes."""

    @model_validator(mode="after")
    def _build_objects(self) -> "WriteExcel":
        self._receiver = ExcelReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Write a single row and yield it."""
        await self._receiver.write_row(
            self.filepath,
            metrics=metrics,
            row=row,
            sheet_name=self.sheet_name,
        )
        yield row

    async def process_bulk(
        self, dataframe: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[pd.DataFrame, None]:
        """Write full pandas DataFrame and yield it."""
        await self._receiver.write_bulk(
            self.filepath,
            metrics=metrics,
            data=dataframe,
            sheet_name=self.sheet_name,
        )
        yield dataframe

    async def process_bigdata(
        self, dataframe: dd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[dd.DataFrame, None]:
        """Write Dask DataFrame and yield it."""
        await self._receiver.write_bigdata(
            self.filepath,
            metrics=metrics,
            data=dataframe,
            sheet_name=self.sheet_name,
        )
        yield dataframe
