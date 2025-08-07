from typing import Any, Dict, List, Literal
from pydantic import Field, model_validator

from src.components.file_components.csv.csv_component import CSV
from src.components.dataclasses import Layout, MetaData
from src.components.registry import register_component
from src.receivers.files.csv_receiver import CSVReceiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics


@register_component("write_csv")
class WriteCSV(CSV):
    """Component that writes data to a CSV file."""

    type: Literal["write_csv"] = Field(default="write_csv")

    @model_validator(mode="after")
    def _build_objects(self):
        """
        Initialize dependent objects after the component is constructed.
        """
        self._receiver = CSVReceiver(filepath=self.filepath, separator=self.separator)
        self.layout = Layout()
        self.metadata = MetaData()
        return self

    async def process_row(
            self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> Dict[str, Any]:
        """Write a single row to the CSV file."""
        await self._receiver.write_row(row=row, metrics=metrics)
        return row

    async def process_bulk(
            self, data: List[Dict[str, Any]], metrics: ComponentMetrics
    ) -> List[Dict[str, Any]]:
        """Write multiple rows to the CSV file."""
        await self._receiver.write_bulk(data=data, metrics=metrics)
        return data

    async def process_bigdata(
            self, chunk_iterable: Any, metrics: ComponentMetrics
    ) -> Any:
        """Write large amounts of data to the CSV file in a streaming fashion."""
        await self._receiver.write_bigdata(data=chunk_iterable, metrics=metrics)
        return chunk_iterable