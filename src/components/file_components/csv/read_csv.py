from typing import Any, Dict, Literal, AsyncGenerator
from pydantic import Field, model_validator

from src.components.file_components.csv.csv_component import CSV
from src.components.dataclasses import Layout, MetaData
from src.components.registry import register_component
from src.receivers.files.csv_receiver import CSVReceiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics


@register_component("read_csv")
class ReadCSV(CSV):
    """Component that reads data from a CSV file."""

    type: Literal["read_csv"] = Field(default="read_csv")

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
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Read a single row from the CSV file."""
        async for result in self._receiver.read_row(metrics=metrics):
            yield result

    async def process_bulk(
            self, data: Any, metrics: ComponentMetrics
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Read the entire CSV file and yield rows one by one."""
        async for result in self._receiver.read_bulk(metrics=metrics):
            yield result

    async def process_bigdata(
            self, chunk_iterable: Any, metrics: ComponentMetrics
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Stream large CSV files in chunks."""
        async for result in self._receiver.read_bigdata(metrics=metrics):
            yield result