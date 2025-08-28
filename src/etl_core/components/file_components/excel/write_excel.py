from __future__ import annotations

from typing import Any, AsyncGenerator, Dict

import dask.dataframe as dd
import pandas as pd
from pydantic import model_validator

from etl_core.components.component_registry import register_component
from etl_core.components.envelopes import Out
from etl_core.components.file_components.excel.excel_component import Excel
from etl_core.components.wiring.ports import InPortSpec, OutPortSpec
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.files.excel.excel_receiver import ExcelReceiver


@register_component("write_excel")
class WriteExcel(Excel):
    """
    Excel writer with port routing.

    - Declares a required input port 'in'.
    - Declares an optional output port 'out' (passthrough), so it can be tested
      in isolation or chained further.
    """

    INPUT_PORTS = (InPortSpec(name="in", required=True, fanin="many"),)
    OUTPUT_PORTS = (OutPortSpec(name="out", required=False, fanout="many"),)

    @model_validator(mode="after")
    def _build_objects(self) -> "WriteExcel":
        self._receiver = ExcelReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Write a single row and emit it on 'out'."""
        await self._receiver.write_row(
            self.filepath,
            metrics=metrics,
            row=row,
            sheet_name=self.sheet_name,
        )
        yield Out(port="out", payload=row)

    async def process_bulk(
        self, dataframe: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Write a pandas DataFrame and emit it on 'out'."""
        await self._receiver.write_bulk(
            self.filepath,
            metrics=metrics,
            data=dataframe,
            sheet_name=self.sheet_name,
        )
        yield Out(port="out", payload=dataframe)

    async def process_bigdata(
        self, dataframe: dd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Write a Dask DataFrame and emit it on 'out'."""
        await self._receiver.write_bigdata(
            self.filepath,
            metrics=metrics,
            data=dataframe,
            sheet_name=self.sheet_name,
        )
        yield Out(port="out", payload=dataframe)
