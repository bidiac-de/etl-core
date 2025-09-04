from __future__ import annotations

from typing import Any, AsyncIterator, Dict, Tuple, Union

import dask.dataframe as dd
import pandas as pd
from pydantic import model_validator, ConfigDict

from etl_core.components.component_registry import register_component
from etl_core.components.data_operations.data_operations import DataOperationsComponent
from etl_core.receivers.data_operations_receivers.merge.merge_receiver import (
    MergeReceiver,
)
from etl_core.job_execution.job_execution_handler import InTagged, Out
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa E501
    DataOperationsMetrics,
)
from etl_core.components.wiring.ports import InPortSpec, OutPortSpec


@register_component("merge")
class MergeComponent(DataOperationsComponent):
    """
    Merge component: routes payloads from multiple named input
    ports to a single output port.
    The component does not combine/aggregate — it simply forwards
    each incoming payload
    to the configured single output. Useful to unite multiple
    sources into a single downstream path.
    Config:
      - inputs: list[str]  (names of input ports)
      - output: str        (name of single output port)
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

    INPUT_PORTS: Tuple[InPortSpec, ...] = (
        InPortSpec(name="in", required=True, fanin="many"),
    )
    OUTPUT_PORTS: Tuple[OutPortSpec, ...] = (
        OutPortSpec(name="merge", required=True, fanout="one"),
    )

    @model_validator(mode="after")
    def _build_objects(self):
        self._receiver = MergeReceiver()
        return self

    async def process_row(
        self,
        row: Dict[str, Any],
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        """Forward a single row (or tagged envelope) to the single output port."""
        async for port_spec, payload_out in self._receiver.process_row(
            out_port=self.OUTPUT_PORTS[0], row=row, metrics=metrics
        ):
            yield Out(port=port_spec.name, payload=payload_out)

    async def process_bulk(
        self,
        dataframe: Union[pd.DataFrame, InTagged],
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        """Forward a DataFrame (or tagged envelope) to the single output port."""

        async for port_spec, payload_out in self._receiver.process_bulk(
            out_port=self.OUTPUT_PORTS[0], dataframe=dataframe, metrics=metrics
        ):
            yield Out(port=port_spec.name, payload=payload_out)

    async def process_bigdata(
        self,
        ddf: Union[dd.DataFrame, InTagged],
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        """Forward a Dask DataFrame (or tagged envelope) to the single output port."""

        async for port_spec, payload_out in self._receiver.process_bigdata(
            out_port=self.OUTPUT_PORTS[0], ddf=ddf, metrics=metrics
        ):
            yield Out(port=port_spec.name, payload=payload_out)
