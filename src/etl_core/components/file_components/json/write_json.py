from __future__ import annotations

from typing import Any, Dict, AsyncGenerator

import dask.dataframe as dd
import pandas as pd
from pydantic import model_validator

from etl_core.components.component_registry import register_component
from etl_core.components.envelopes import Out
from etl_core.components.file_components.json.json_component import JSON
from etl_core.components.wiring.ports import InPortSpec, OutPortSpec
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.files.json.json_receiver import JSONReceiver


@register_component("write_json")
class WriteJSON(JSON):
    """
    JSON/NDJSON writer with port routing.

    - Declares a required input port 'in'.
    - Declares an optional passthrough output port 'out' for chaining/tests.
    """

    INPUT_PORTS = (InPortSpec(name="in", required=True, fanin="many"),)
    OUTPUT_PORTS = (OutPortSpec(name="out", required=False, fanout="many"),)

    @model_validator(mode="after")
    def _build_objects(self) -> "WriteJSON":
        self._receiver = JSONReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Write a single row, emit it on 'out'."""
        await self._receiver.write_row(self.filepath, metrics=metrics, row=row)
        yield Out(port="out", payload=row)

    async def process_bulk(
        self, dataframe: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Write a pandas DataFrame, emit it on 'out'."""
        await self._receiver.write_bulk(self.filepath, metrics=metrics, data=dataframe)
        yield Out(port="out", payload=dataframe)

    async def process_bigdata(
        self, dataframe: dd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Write a Dask DataFrame, emit it on 'out'."""
        await self._receiver.write_bigdata(
            self.filepath, metrics=metrics, data=dataframe
        )
        yield Out(port="out", payload=dataframe)
