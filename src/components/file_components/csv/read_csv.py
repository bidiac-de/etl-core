from typing import Any, Dict, AsyncGenerator, Literal
import pandas as pd
import dask.dataframe as dd
from pydantic import Field, model_validator

from src.components.file_components.csv.csv_component import CSV
from src.components.registry import register_component
from src.receivers.files.csv.csv_receiver import CSVReceiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics


@register_component("read_csv")
class ReadCSV(CSV):
    """CSV reader supporting row, bulk, and bigdata modes."""

    @model_validator(mode="after")
    def _build_objects(self):
        self._receiver = CSVReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Read rows one-by-one (streaming)."""
        sep = getattr(self.separator, "value", self.separator)
        async for result in self._receiver.read_row(
            self.filepath, metrics=metrics, separator=sep
        ):
            yield result

    async def process_bulk(
        self,
        dataframe: pd.DataFrame,
        metrics: ComponentMetrics
    ) -> AsyncGenerator[pd.DataFrame, None]:
        """Read whole CSV as a pandas DataFrame."""
        df = await self._receiver.read_bulk(self.filepath, metrics=metrics)
        yield df

    async def process_bigdata(
        self,
        dataframe: pd.DataFrame,
        metrics: ComponentMetrics
    ) -> AsyncGenerator[dd.DataFrame, None]:
        """Read large CSV as a Dask DataFrame."""
        ddf = await self._receiver.read_bigdata(self.filepath, metrics=metrics)
        yield ddf
