from __future__ import annotations

from typing import Any, AsyncGenerator, Dict, List

import dask.dataframe as dd
import pandas as pd
from pydantic import model_validator

from src.components.file_components.excel.excel_component import Excel
from src.components.component_registry import register_component
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.files.excel.excel_receiver import ExcelReceiver


@register_component("write_excel")
class WriteExcel(Excel):
    """Excel writer supporting row, bulk, and bigdata modes (yield-only)."""

    _receiver: ExcelReceiver | None = None

    # --- required by abstract base (Component) ---
    def _build_objects(self) -> "WriteExcel":
        """Initialize receiver; called by frameworks expecting this hook."""
        self._receiver = ExcelReceiver()
        return self

    @model_validator(mode="after")
    def _after_validate(self) -> "WriteExcel":
        """Writers may create new files: no exists() check."""
        if self._receiver is None:
            self._receiver = ExcelReceiver()
        return self

    async def process_row(
            self,
            row: Dict[str, Any],
            metrics: ComponentMetrics,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Write a single row and yield it."""
        assert self._receiver is not None
        async for out in self._receiver.write_row(
                filepath=self.filepath,
                sheet_name=self.sheet_name,
                row=row,
                metrics=metrics,
        ):
            yield out

    async def process_bulk(
            self,
            data: pd.DataFrame | List[Dict[str, Any]] | None,
            metrics: ComponentMetrics,
    ) -> AsyncGenerator[pd.DataFrame, None]:
        """Write a full DataFrame (or list-of-dicts) and yield the DataFrame."""
        assert self._receiver is not None
        async for df in self._receiver.write_bulk(
                filepath=self.filepath,
                sheet_name=self.sheet_name,
                data=data,
                metrics=metrics,
        ):
            yield df

    async def process_bigdata(
            self,
            chunk_iterable: dd.DataFrame,
            metrics: ComponentMetrics,
    ) -> AsyncGenerator[dd.DataFrame, None]:
        """Write a Dask DataFrame and yield it (unchanged)."""
        assert self._receiver is not None
        async for ddf in self._receiver.write_bigdata(
                filepath=self.filepath,
                sheet_name=self.sheet_name,
                data=chunk_iterable,
                metrics=metrics,
        ):
            yield ddf