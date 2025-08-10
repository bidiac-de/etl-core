from typing import Any, Dict, AsyncGenerator, Literal
import pandas as pd
import dask.dataframe as dd
from pydantic import Field, model_validator

from src.components.file_components.csv.csv_component import CSV
from src.components.registry import register_component
from src.receivers.files.csv_receiver import CSVReceiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics

@register_component("read_csv")
class ReadCSV(CSV):
    type: Literal["write_csv"] = Field(default="write_csv")

    @model_validator(mode="after")
    def _build_objects(self):
        # Receiver ist STATELESS – kein filepath/sep im ctor
        self._receiver = CSVReceiver()
        return self

    async def process_row(
            self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Read rows one-by-one (streaming)."""
        # ❗ WICHTIG: filepath an den Receiver übergeben
        async for result in self._receiver.read_row(self.filepath, metrics=metrics):
            yield result

    async def process_bulk(
            self, data: Any, metrics: ComponentMetrics
    ) -> pd.DataFrame:
        """Read whole CSV as a pandas DataFrame."""
        # ❗ KEIN async for — DataFrame direkt zurückgeben
        return await self._receiver.read_bulk(self.filepath, metrics=metrics)

    async def process_bigdata(
            self, chunk_iterable: Any, metrics: ComponentMetrics
    ) -> dd.DataFrame:
        """Read large CSV as a Dask DataFrame."""
        return await self._receiver.read_bigdata(self.filepath, metrics=metrics)