from typing import Any, Dict, Literal, AsyncGenerator
from pydantic import Field, model_validator
import pandas as pd

from src.components.file_components.json.json_component import JSON
from src.components.dataclasses import Layout, MetaData
from src.components.registry import register_component
from src.receivers.files.json_receiver import JSONReceiver
from src.metrics.component_metrics import ComponentMetrics


@register_component("read_json")
class ReadJSON(JSON):
    """Component that reads data from a JSON/NDJSON file (async streaming)."""

    type: Literal["read_json"] = Field(default="read_json")

    @model_validator(mode="after")
    def build_objects(self):
        self._receiver = JSONReceiver(filepath=self.filepath)
        self.layout = Layout()
        self.metadata = MetaData()
        return self

    async def process_row(
            self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Yield one row (dict) at a time.
        """
        async for rec in self._receiver.read_row(metrics=metrics):
            yield rec

    async def process_bulk(
            self, data: Any, metrics: ComponentMetrics
    ) -> AsyncGenerator[pd.DataFrame, None]:
        """
        Yield pandas DataFrame-Chunks.
        """
        async for df in self._receiver.read_bulk(metrics=metrics):
            yield df

    async def process_bigdata(
            self, chunk_iterable: Any, metrics: ComponentMetrics
    ) -> AsyncGenerator[pd.DataFrame, None]:
        """
        Yield pandas DataFrame pro (Dask-)Partition.
        """
        async for df in self._receiver.read_bigdata(metrics=metrics):
            yield df