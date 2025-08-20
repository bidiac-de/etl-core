from typing import Any, Dict, AsyncGenerator
import pandas as pd
import dask.dataframe as dd
from pydantic import model_validator

from etl_core.components.file_components.csv.csv_component import CSV
from etl_core.components.component_registry import register_component
from etl_core.receivers.files.csv.csv_receiver import CSVReceiver
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics


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
        async for result in self._receiver.read_row(
            self.filepath, metrics=metrics, separator=self.separator
        ):
            yield result

    async def process_bulk(
        self, dataframe: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[pd.DataFrame, None]:
        """Read whole CSV as a pandas DataFrame."""
        df = await self._receiver.read_bulk(
            self.filepath, metrics=metrics, separator=self.separator
        )
        yield df

    async def process_bigdata(
        self, dataframe: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[dd.DataFrame, None]:
        """Read large CSV as a Dask DataFrame."""
        ddf = await self._receiver.read_bigdata(
            self.filepath, metrics=metrics, separator=self.separator
        )
        yield ddf
