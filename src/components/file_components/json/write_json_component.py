from pathlib import Path
from typing import Any, Dict, List, Literal
from pydantic import Field

from src.components.file_components.json.json_component import JSON
from src.components.column_definition import ColumnDefinition
from src.components.dataclasses import Layout, MetaData
from src.components.base_components import get_strategy
from src.components.registry import register_component
from src.receivers.files.json_receiver import JSONReceiver
from src.metrics.component_metrics import ComponentMetrics


@register_component("write_json")
class WriteJSON(JSON):
    """Component that writes data to a JSON file."""

    type: Literal["write_json"] = Field(default="write_json")

    @classmethod
    def build_objects(cls, values: dict) -> dict:
        """Initialize layout, strategy, receiver, and metadata for the component."""
        values.setdefault("layout", Layout())
        values["strategy"] = get_strategy(values["strategy_type"])
        values["receiver"] = JSONReceiver()
        values.setdefault("metadata", MetaData())
        return values

    def process_row(self, row: Dict[str, Any], metrics: ComponentMetrics) -> Dict[str, Any]:
        """Write a single row to the JSON file."""
        self.receiver.write_row(row=row, filepath=self.filepath, metrics=metrics)
        return row

    def process_bulk(self, data: List[Dict[str, Any]], metrics: ComponentMetrics) -> List[Dict[str, Any]]:
        """Write multiple rows to the JSON file."""
        self.receiver.write_bulk(df=data, filepath=self.filepath, metrics=metrics)
        return data

    def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics) -> Any:
        """Write large amounts of data to the JSON file using a streaming approach."""
        self.receiver.write_bigdata(ddf=chunk_iterable, filepath=self.filepath, metrics=metrics)
        return chunk_iterable

