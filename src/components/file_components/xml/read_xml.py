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
    type: Literal["read_xml"] = Field(default="read_xml")

    @model_validator(mode="after")
    def _build_objects(self):
        self._receiver = XMLReceiver(root_tag=self.root_tag, record_tag=self.record_tag)
        return self

    async def process_row(self, row: Dict[str, Any], metrics: ComponentMetrics) -> AsyncGenerator[Dict[str, Any], None]:
        async for rec in self._receiver.read_row(
                self.filepath, metrics, root_tag=self.root_tag, record_tag=self.record_tag
        ):
            yield rec

    async def process_bulk(self, data: Any, metrics: ComponentMetrics) -> pd.DataFrame:
        return await self._receiver.read_bulk(
            self.filepath, metrics, root_tag=self.root_tag, record_tag=self.record_tag
        )

    async def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics) -> dd.DataFrame:
        return await self._receiver.read_bigdata(
            self.filepath, metrics, root_tag=self.root_tag, record_tag=self.record_tag
        )