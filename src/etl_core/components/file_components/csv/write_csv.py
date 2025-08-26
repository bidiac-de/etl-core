from typing import Any, Dict, AsyncGenerator
import pandas as pd
import dask.dataframe as dd
from pydantic import model_validator

from etl_core.components.file_components.csv.csv_component import CSV
from etl_core.components.component_registry import register_component
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.files.csv.csv_receiver import CSVReceiver
from etl_core.components.envelopes import Out
from etl_core.components.wiring.ports import InPortSpec, OutPortSpec


@register_component("write_csv")
class WriteCSV(CSV):
    """
    CSV writer supporting row, bulk, and bigdata modes with port routing.

    Notes:
    - Declares one input port 'in' (required).
    - Declares a non-required passthrough output port 'out' so tests can await
      a first item and the runtime can still route if user wires a successor.
      If you prefer a pure sink, you can remove OUTPUT_PORTS and simply not yield.
    """

    INPUT_PORTS = (InPortSpec(name="in", required=True, fanin="many"),)
    OUTPUT_PORTS = (OutPortSpec(name="out", required=False, fanout="many"),)

    @model_validator(mode="after")
    def _build_objects(self):
        self._receiver = CSVReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Write a single row, emit the written row on 'out'."""
        await self._receiver.write_row(
            self.filepath, metrics=metrics, row=row, separator=self.separator
        )
        yield Out(port="out", payload=row)

    async def process_bulk(
        self, dataframe: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Write full pandas DataFrame, emit it on 'out'."""
        await self._receiver.write_bulk(
            self.filepath, metrics=metrics, data=dataframe, separator=self.separator
        )
        yield Out(port="out", payload=dataframe)

    async def process_bigdata(
        self, dataframe: dd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Write Dask DataFrame, emit it on 'out'."""
        await self._receiver.write_bigdata(
            self.filepath, metrics=metrics, data=dataframe, separator=self.separator
        )
        yield Out(port="out", payload=dataframe)
