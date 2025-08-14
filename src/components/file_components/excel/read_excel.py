from __future__ import annotations
from pathlib import Path
from typing import Any, AsyncGenerator, Dict

import dask.dataframe as dd
import pandas as pd
from pydantic import model_validator

from src.components.file_components.excel.excel_component import Excel
from src.components.component_registry import  register_component
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.files.excel.excel_receiver import ExcelReceiver


@register_component("read_excel")
class ReadExcel(Excel):
    """Excel reader supporting row, bulk, and bigdata modes (yield-only)."""

    _receiver: ExcelReceiver | None = None


    def _build_objects(self) -> "ReadExcel":
        """Initialize receiver; called by frameworks expecting this hook."""
        self._receiver = ExcelReceiver()
        return self

    @model_validator(mode="after")
    def _after_validate(self) -> "ReadExcel":
        """Validate filepath (must exist) and ensure receiver is set."""
        if not Path(self.filepath).exists():
            raise ValueError(f"Filepath does not exist: {self.filepath}")
        if self._receiver is None:
            self._receiver = ExcelReceiver()
        return self

    async def process_row(
            self,
            row: Dict[str, Any],
            metrics: ComponentMetrics,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Yield one row (dict) at a time."""
        assert self._receiver is not None
        async for rec in self._receiver.read_row(
                filepath=self.filepath,
                sheet_name=self.sheet_name,
                metrics=metrics,
        ):
            yield rec

    async def process_bulk(
            self,
            data: Any,
            metrics: ComponentMetrics,
    ) -> AsyncGenerator[pd.DataFrame, None]:
        """Yield a single pandas DataFrame."""
        assert self._receiver is not None
        async for df in self._receiver.read_bulk(
                filepath=self.filepath,
                sheet_name=self.sheet_name,
                metrics=metrics,
        ):
            yield df

    async def process_bigdata(
            self,
            chunk_iterable: Any,
            metrics: ComponentMetrics,
    ) -> AsyncGenerator[dd.DataFrame, None]:
        """Yield a single Dask DataFrame (lazy)."""
        assert self._receiver is not None
        async for ddf in self._receiver.read_bigdata(
                filepath=self.filepath,
                sheet_name=self.sheet_name,
                metrics=metrics,
        ):
            yield ddf