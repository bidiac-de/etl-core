from typing import Any, AsyncGenerator, Dict

import pandas as pd
import dask.dataframe as dd
from pydantic import model_validator

from etl_core.components.file_components.excel.excel_component import Excel
from src.etl_core.components.component_registry import register_component
from etl_core.receivers.files.excel.excel_receiver import ExcelReceiver
from src.etl_core.metrics.component_metrics.component_metrics import ComponentMetrics


@register_component("read_excel")
class ReadExcel(Excel):
    """Excel reader supporting row, bulk, and bigdata modes."""

    @model_validator(mode="after")
    def _build_objects(self) -> "ReadExcel":
        self._receiver = ExcelReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Read rows one-by-one (streaming) from the configured sheet."""
        async for result in self._receiver.read_row(
            self.filepath, metrics=metrics, sheet_name=self.sheet_name
        ):
            yield result

    async def process_bulk(
        self,
        dataframe: pd.DataFrame,
        metrics: ComponentMetrics,
    ) -> AsyncGenerator[pd.DataFrame, None]:
        """Read the entire sheet into a pandas DataFrame."""
        df = await self._receiver.read_bulk(
            self.filepath, metrics=metrics, sheet_name=self.sheet_name
        )
        yield df

    async def process_bigdata(
        self,
        dataframe: dd.DataFrame,
        metrics: ComponentMetrics,
    ) -> AsyncGenerator[dd.DataFrame, None]:
        """Read the sheet as a Dask DataFrame."""
        ddf = await self._receiver.read_bigdata(
            self.filepath, metrics=metrics, sheet_name=self.sheet_name
        )
        yield ddf
