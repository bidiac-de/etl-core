from typing import Any, Dict, List, Literal, Union, AsyncGenerator
from pydantic import Field, model_validator
import pandas as pd

from src.components.file_components.json.json_component import JSON
from src.components.dataclasses import Layout, MetaData
from src.components.registry import register_component
from src.receivers.files.json_receiver import JSONReceiver
from src.metrics.component_metrics import ComponentMetrics


@register_component("write_json")
class WriteJSON(JSON):
    """Component that writes data to a JSON/NDJSON file (async streaming)."""

    type: Literal["write_json"] = Field(default="write_json")

    @model_validator(mode="after")
    def _build_objects(self):
        self._receiver = JSONReceiver()
        return self

    async def process_row(
            self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Write a single row and pass it downstream.
        """
        await self._receiver.write_row(self.filepath, metrics=metrics, row=row)
        yield row

    async def process_bulk(
            self, data: Union[List[Dict[str, Any]], pd.DataFrame], metrics: ComponentMetrics
    ) -> AsyncGenerator[Union[List[Dict[str, Any]], pd.DataFrame], None]:
        """
        Write multiple rows (DataFrame or List[dict]).
        """
        await self._receiver.write_bulk(self.filepath, metrics=metrics, data=data)
        yield data

    async def process_bigdata(
            self, chunk_iterable: Any, metrics: ComponentMetrics
    ) -> AsyncGenerator[Any, None]:
        """
        Write big data (z. B. Dask DataFrame)."""
        await self._receiver.write_bigdata(self.filepath, metrics=metrics, data=chunk_iterable)
        yield chunk_iterable

