from typing import Any, Dict, AsyncGenerator
from pydantic import model_validator
import pandas as pd
import dask.dataframe as dd

from etl_core.components.file_components.json.json_component import JSON
from etl_core.components.registry import register_component
from etl_core.receivers.files.json.json_receiver import JSONReceiver
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics


@register_component("write_json")
class WriteJSON(JSON):
    """Component that writes data to a JSON/NDJSON file (async streaming)."""

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
        self, dataframe: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[pd.DataFrame, None]:
        """
        Write multiple rows (DataFrame or List[dict]).
        """
        await self._receiver.write_bulk(self.filepath, metrics=metrics, data=dataframe)
        yield dataframe

    async def process_bigdata(
        self, dataframe: dd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncGenerator[dd.DataFrame, None]:
        """
        Write big data (z. B. Dask DataFrame)."""
        await self._receiver.write_bigdata(
            self.filepath, metrics=metrics, data=dataframe
        )
        yield dataframe
