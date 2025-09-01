from typing import Any, Dict, AsyncGenerator, ClassVar
import pandas as pd
from pydantic import model_validator

from etl_core.components.file_components.csv.csv_component import CSV
from etl_core.components.component_registry import register_component
from etl_core.receivers.files.csv.csv_receiver import CSVReceiver
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.components.envelopes import Out
from etl_core.components.wiring.ports import OutPortSpec


@register_component("read_csv")
class ReadCSV(CSV):
    """CSV reader supporting row, bulk, and bigdata modes with port routing."""

    ALLOW_NO_INPUTS: ClassVar[bool] = True
    OUTPUT_PORTS = (OutPortSpec(name="out", required=True, fanout="many"),)

    @model_validator(mode="after")
    def _build_objects(self):
        self._receiver = CSVReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Stream CSV rows one-by-one and emit on the "out" port."""
        async for result in self._receiver.read_row(
            self.filepath, metrics=metrics, separator=self.separator
        ):
            yield Out(port="out", payload=result)

    async def process_bulk(
        self, dataframe: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Read whole CSV as a pandas DataFrame and emit on "out"."""
        df = await self._receiver.read_bulk(
            self.filepath, metrics=metrics, separator=self.separator
        )
        yield Out(port="out", payload=df)

    async def process_bigdata(
        self, dataframe: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[Out, None]:
        """Read large CSV as a Dask DataFrame and emit on "out"."""
        ddf = await self._receiver.read_bigdata(
            self.filepath, metrics=metrics, separator=self.separator
        )
        yield Out(port="out", payload=ddf)
