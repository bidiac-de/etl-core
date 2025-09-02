from __future__ import annotations

from typing import Any, AsyncIterator, Dict, Tuple

import dask.dataframe as dd
import pandas as pd
from pydantic import ConfigDict, PrivateAttr, model_validator

from etl_core.components.component_registry import register_component
from etl_core.components.data_operations.data_operations import DataOperationsComponent
from etl_core.receivers.data_flow.split_receiver import SplitReceiver
from etl_core.job_execution.job_execution_handler import Out
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (
    DataOperationsMetrics,
)
from etl_core.components.wiring.ports import InPortSpec, OutPortSpec


@register_component("split")
class SplitComponent(DataOperationsComponent):
    """
    Duplicates incoming payloads to every defined OUTPUT_PORT.

    INPUT:
      - 'in' (required)
    OUTPUT:
      - All ports declared in OUTPUT_PORTS.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

    INPUT_PORTS = (InPortSpec(name="in", required=True, fanin="one"),)
    OUTPUT_PORTS: Tuple[OutPortSpec, ...] = ()

    _receiver: SplitReceiver = PrivateAttr()

    @model_validator(mode="after")
    def _build_objects(self) -> "SplitComponent":
        self._receiver = SplitReceiver()
        if not self.OUTPUT_PORTS:
            raise ValueError(f"SplitComponent '{self.name}' requires at least one OUTPUT_PORT.")
        return self

    async def process_row(self, row: Dict[str, Any], metrics: DataOperationsMetrics) -> AsyncIterator[Out]:
        async for port, dup in self._receiver.process_row(row=row, branches=self.OUTPUT_PORTS, metrics=metrics):
            yield Out(port=port.name, payload=dup)

    async def process_bulk(self, dataframe: pd.DataFrame, metrics: DataOperationsMetrics) -> AsyncIterator[Out]:
        async for port, dup in self._receiver.process_bulk(dataframe=dataframe, branches=self.OUTPUT_PORTS, metrics=metrics):
            yield Out(port=port.name, payload=dup)

    async def process_bigdata(self, ddf: dd.DataFrame) -> AsyncIterator[Out]:
        async for port, dup in self._receiver.process_bigdata(ddf=ddf, branches=self.OUTPUT_PORTS):
            yield Out(port=port.name, payload=dup)