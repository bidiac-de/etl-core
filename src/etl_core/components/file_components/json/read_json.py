from __future__ import annotations

from typing import Any, Dict, AsyncGenerator, ClassVar

import dask.dataframe as dd
import pandas as pd
from pydantic import model_validator

from etl_core.components.component_registry import register_component
from etl_core.components.envelopes import Out
from etl_core.components.file_components.json.json_component import JSON
from etl_core.components.wiring.ports import OutPortSpec
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.files.json.json_receiver import JSONReceiver


@register_component("read_json")
class ReadJSON(JSON):
    """
    JSON/NDJSON reader supporting row, bulk, and bigdata modes with port routing.
    """

    ALLOW_NO_INPUTS: ClassVar[bool] = True
    OUTPUT_PORTS = (OutPortSpec(name="out", required=True, fanout="many"),)

    @model_validator(mode="after")
    def _build_objects(self) -> "ReadJSON":
        self._receiver = JSONReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Stream NDJSON/JSON rows one-by-one and emit on 'out'."""
        async for result in self._receiver.read_row(self.filepath, metrics=metrics):
            yield Out(port="out", payload=result)

    async def process_bulk(
        self, data: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Read JSON/NDJSON into a pandas DataFrame and emit on 'out'."""
        dataframe = await self._receiver.read_bulk(self.filepath, metrics=metrics)
        yield Out(port="out", payload=dataframe)

    async def process_bigdata(
        self, data: dd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Read JSON/NDJSON as a Dask DataFrame and emit on 'out'."""
        dataframe = await self._receiver.read_bigdata(self.filepath, metrics=metrics)
        yield Out(port="out", payload=dataframe)
