from __future__ import annotations
from typing import Any, Dict, Literal, AsyncGenerator
from pydantic import Field, model_validator
import pandas as pd
import dask.dataframe as dd

from src.components.file_components.xml.xml_component import XML
from src.components.registry import register_component
from src.receivers.files.xml_receiver import XMLReceiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics

@register_component("read_xml")
class ReadXML(XML):
    """Component for reading from XML – async streaming similar to JSON."""
    type: Literal["read_xml"] = "read_xml"

    @model_validator(mode="after")
    def _build_objects(self):
        self._receiver = XMLReceiver()
        return self

    async def process_row(
            self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncGenerator[Dict[str, Any], None]:
        async for rec in self._receiver.read_row(
                self.filepath, metrics=metrics, root_tag=self.root_tag, record_tag=self.record_tag
        ):
            yield rec

    async def process_bulk(
            self, data: Any, metrics: ComponentMetrics
    ) -> AsyncGenerator[pd.DataFrame, None]:
        df = await self._receiver.read_bulk(
            self.filepath, metrics=metrics, root_tag=self.root_tag, record_tag=self.record_tag
        )
        yield df

    async def process_bigdata(
            self, chunk_iterable: Any, metrics: ComponentMetrics
    ) -> AsyncGenerator[dd.DataFrame, None]:
        ddf = await self._receiver.read_bigdata(
            self.filepath, metrics=metrics, root_tag=self.root_tag, record_tag=self.record_tag
        )
        yield ddf