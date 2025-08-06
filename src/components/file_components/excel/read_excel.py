from typing import Any, Dict, List, Literal
from pydantic import Field

from src.components.file_components.excel.excel_component import Excel
from src.components.dataclasses import Layout, MetaData
from src.components.base_component import get_strategy
from src.components.component_registry import register_component
from src.receivers.files.excel_receiver import ExcelReceiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics


@register_component("read_excel")
class ReadExcel(Excel):
    """Component that reads data from an Excel file."""

    type: Literal["read_excel"] = Field(default="read_excel")

    @classmethod
    def build_objects(cls, values):
        values.setdefault("layout", Layout())
        values["strategy"] = get_strategy(values["strategy_type"])
        values["receiver"] = ExcelReceiver(filepath=values["filepath"])
        values.setdefault("metadata", MetaData())
        return values

    def process_row(self, row: Dict[str, Any], metrics: ComponentMetrics) -> Dict[str, Any]:
        return self.receiver.read_row(metrics=metrics)

    def process_bulk(self, data: List[Dict[str, Any]], metrics: ComponentMetrics) -> List[Dict[str, Any]]:
        return self.receiver.read_bulk(metrics=metrics)

    def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics) -> Any:
        return self.receiver.read_bigdata(metrics=metrics)