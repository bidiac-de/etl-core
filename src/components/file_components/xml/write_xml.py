from typing import Any, Dict, List, Literal

import pandas as pd
from pydantic import Field, model_validator
import dask.dataframe as dd

from src.components.file_components.xml.xml_component import XML
from src.components.registry import register_component
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.files.xml_receiver import XMLReceiver

@register_component("write_xml")
class WriteXML(XML):
    type: Literal["write_xml"] = Field(default="write_xml")

    @model_validator(mode="after")
    def _build_objects(self):
        self._receiver = XMLReceiver(root_tag=self.root_tag, record_tag=self.record_tag)
        return self

    async def process_row(self, row: Dict[str, Any], metrics: ComponentMetrics) -> Dict[str, Any]:
        await self._receiver.write_row(
            self.filepath, metrics, row=row, root_tag=self.root_tag, record_tag=self.record_tag
        )
        return row

    async def process_bulk(self, data: List[Dict[str, Any]] | pd.DataFrame, metrics: ComponentMetrics):
        if isinstance(data, pd.DataFrame):
            payload = data.to_dict(orient="records")
        else:
            payload = data
        await self._receiver.write_bulk(
            self.filepath, metrics, data=payload, root_tag=self.root_tag, record_tag=self.record_tag
        )
        return data

    async def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics) -> dd.DataFrame:
        ddf: dd.DataFrame = chunk_iterable
        await self._receiver.write_bigdata(
            self.filepath, metrics, data=ddf, record_tag=self.record_tag
        )
        return ddf
