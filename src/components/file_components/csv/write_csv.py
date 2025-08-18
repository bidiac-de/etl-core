from typing import Any, Dict, List, AsyncGenerator, Literal
import pandas as pd
import dask.dataframe as dd
from pydantic import model_validator

from src.components.file_components.csv.csv_component import CSV
from src.components.registry import register_component
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.files.csv.csv_receiver import CSVReceiver


@register_component("write_csv")
class WriteCSV(CSV):
    """CSV writer supporting row, bulk, and bigdata modes."""

    @model_validator(mode="after")
    def _build_objects(self):
        self._receiver = CSVReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Write a single row and yield it."""
        await self._receiver.write_row(self.filepath, metrics=metrics, row=row, separator=self.separator)
        yield row

    async def process_bulk(
        self, dataframe: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[pd.DataFrame, None]:
        """Write full pandas DataFrame and yield it."""
        await self._receiver.write_bulk(self.filepath, metrics=metrics, data=dataframe, separator=self.separator)
        yield dataframe

    async def process_bigdata(
        self, dataframe: dd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[dd.DataFrame, None]:
        """Write Dask DataFrame and yield it."""
        await self._receiver.write_bigdata(
            self.filepath, metrics=metrics, data=dataframe, separator=self.separator
        )
        yield dataframe
