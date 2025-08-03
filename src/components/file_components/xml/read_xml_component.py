from abc import ABC
from typing import Any, Dict, List, Literal, Optional
from pandas import DataFrame
from pydantic import Field, PrivateAttr, computed_field

from src.components.file_components.xml.xml_component import XML
from src.components.dataclasses import Layout, MetaData
from src.components.base_component import get_strategy
from src.components.registry import register_component
from src.receivers.files.xml_receiver import XMLReceiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics

@register_component("read_xml")
class ReadXML(XML, ABC):
    """Component that reads data from an XML file."""
    _type: Literal["read_xml"] = PrivateAttr(default="read_xml")

    @computed_field(return_type=Literal["read_xml"])
    @property
    def type(self) -> Literal["read_xml"]:
        return self._type

    @classmethod
    def build_objects(cls, values: dict) -> dict:
        values.setdefault("layout", Layout())
        values["strategy"] = get_strategy(values["strategy_type"])
        values["receiver"] = XMLReceiver()
        values.setdefault("metadata", MetaData())
        return values

    def process_row(self, row: Dict[str, Any], metrics: ComponentMetrics = None) -> Dict[str, Any]:
        m = metrics or self.metrics
        return self.receiver.read_row(filepath=self.filepath, metrics=m)

    def process_bulk(self, data: List[Dict[str, Any]], metrics: ComponentMetrics = None) -> DataFrame:
        m = metrics or self.metrics
        return self.receiver.read_bulk(filepath=self.filepath, metrics=m)

    def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics = None):
        m = metrics or self.metrics
        return self.receiver.read_bigdata(filepath=self.filepath, metrics=m)
