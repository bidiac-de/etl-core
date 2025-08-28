from __future__ import annotations

from typing import Any, AsyncGenerator, Dict, ClassVar

import dask.dataframe as dd
import pandas as pd
from pydantic import model_validator

from etl_core.components.component_registry import register_component
from etl_core.components.envelopes import Out
from etl_core.components.file_components.excel.excel_component import Excel
from etl_core.components.wiring.ports import OutPortSpec
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.files.excel.excel_receiver import ExcelReceiver


@register_component("read_excel")
class ReadExcel(Excel):
    """Excel reader supporting row, bulk, and bigdata modes with port routing."""

    ALLOW_NO_INPUTS: ClassVar[bool] = True
    OUTPUT_PORTS = (OutPortSpec(name="out", required=True, fanout="many"),)

    @model_validator(mode="after")
    def _build_objects(self) -> "ReadExcel":
        self._receiver = ExcelReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Read rows one-by-one and emit on 'out'."""
        async for result in self._receiver.read_row(
            self.filepath, metrics=metrics, sheet_name=self.sheet_name
        ):
            yield Out(port="out", payload=result)

    async def process_bulk(
        self, dataframe: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Read the entire sheet into a pandas DataFrame and emit on 'out'."""
        df = await self._receiver.read_bulk(
            self.filepath, metrics=metrics, sheet_name=self.sheet_name
        )
        yield Out(port="out", payload=df)

    async def process_bigdata(
        self, dataframe: dd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Read the sheet as a Dask DataFrame and emit on 'out'."""
        ddf = await self._receiver.read_bigdata(
            self.filepath, metrics=metrics, sheet_name=self.sheet_name
        )
        yield Out(port="out", payload=ddf)
