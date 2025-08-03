from abc import ABC
from typing import Any, Dict, List, Literal, Optional
from pydantic import Field
from src.components.file_components.xml.xml_component import XML
from src.components.dataclasses import Layout, MetaData
from src.components.base_component import get_strategy
from src.components.registry import register_component
from src.receivers.files.xml_receiver import XMLReceiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics

@register_component("write_xml")
class WriteXML(XML, ABC):
    """Component that writes data to an XML file."""
    type: Literal["write_xml"] = Field(default="write_xml")

    @classmethod
    def build_objects(cls, values: dict) -> dict:
        values.setdefault("layout", Layout())
        values["strategy"] = get_strategy(values["strategy_type"])
        values["receiver"] = XMLReceiver()
        values.setdefault("metadata", MetaData())
        return values

    def process_row(self, row: Dict[str, Any], metrics: ComponentMetrics = None) -> Dict[str, Any]:
        m = metrics or self.metrics
        self.receiver.write_row(row=row, filepath=self.filepath, metrics=m)
        return row

    def process_bulk(self, data, metrics: ComponentMetrics = None):
        m = metrics or self.metrics
        self.receiver.write_bulk(df=data, filepath=self.filepath, metrics=m)
        return data

    def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics = None):
        m = metrics or self.metrics
        self.receiver.write_bigdata(chunk_iterable=chunk_iterable, filepath=self.filepath, metrics=m)
        return chunk_iterable
