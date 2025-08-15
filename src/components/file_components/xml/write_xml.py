from __future__ import annotations
from typing import Any, Dict, List, Literal, Union, AsyncGenerator
import pandas as pd
from pydantic import Field, model_validator

from src.components.file_components.xml.xml_component import XML
from src.components.registry import register_component
from src.receivers.files.xml_receiver import XMLReceiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics

@register_component("write_xml")
class WriteXML(XML):
    """Component for writing to XML – async streaming similar to JSON."""
    type: Literal["write_xml"] = Field(default="write_xml")

    @model_validator(mode="after")
    def _build_objects(self):
        self._receiver = XMLReceiver()
        return self

    async def process_row(
            self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncGenerator[Dict[str, Any], None]:
        await self._receiver.write_row(
            self.filepath, metrics=metrics, row=row,
            root_tag=self.root_tag, record_tag=self.record_tag
        )
        yield row

    async def process_bulk(
            self, data: Union[List[Dict[str, Any]], pd.DataFrame], metrics: ComponentMetrics
    ) -> AsyncGenerator[Union[List[Dict[str, Any]], pd.DataFrame], None]:
        await self._receiver.write_bulk(
            self.filepath, metrics=metrics, data=data,
            root_tag=self.root_tag, record_tag=self.record_tag
        )
        yield data

    async def process_bigdata(
            self, chunk_iterable: Any, metrics: ComponentMetrics
    ) -> AsyncGenerator[Any, None]:
        await self._receiver.write_bigdata(
            self.filepath, metrics=metrics, data=chunk_iterable,
            record_tag=self.record_tag
        )
        yield chunk_iterable
