from typing import Any, Dict, AsyncGenerator
from pydantic import model_validator
import pandas as pd
import dask.dataframe as dd

from src.components.file_components.json.json_component import JSON
from src.components.registry import register_component
from src.receivers.files.json.json_receiver import JSONReceiver
from src.metrics.component_metrics import ComponentMetrics


@register_component("read_json")
class ReadJSON(JSON):
    """Component that reads data from a JSON/NDJSON file (async streaming)."""

    @model_validator(mode="after")
    def _build_objects(self):
        self._receiver = JSONReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Yield one row (dict) at a time.
        """
        async for result in self._receiver.read_row(self.filepath, metrics=metrics):
            yield result

    async def process_bulk(
        self, data: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[pd.DataFrame, None]:
        """
        Yield pandas DataFrame-Chunks.
        """
        dataframe = await self._receiver.read_bulk(self.filepath, metrics=metrics)
        yield dataframe

    async def process_bigdata(
        self, data: dd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[dd.DataFrame, None]:
        """
        Yield pandas DataFrame pro (Dask-)Partition.
        """
        dataframe = await self._receiver.read_bigdata(self.filepath, metrics=metrics)
        yield dataframe
