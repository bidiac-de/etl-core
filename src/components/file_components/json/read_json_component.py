from pathlib import Path
from typing import Any, Dict, List, Literal

from pandas import DataFrame
from pydantic import Field, PrivateAttr, computed_field

from src.components.file_components.json.json_component import JSON
from src.components.column_definition import ColumnDefinition
from src.components.dataclasses import Layout, MetaData
from src.components.base_components import get_strategy
from src.components.registry import register_component
from src.receivers.files.json_receiver import JSONReceiver
from src.metrics.component_metrics import ComponentMetrics


@register_component("read_json")
class ReadJSON(JSON):
    """Component that reads data from a JSON file."""

    _type: Literal["read_json"] = PrivateAttr(default="read_json")

    @computed_field(return_type=Literal["read_json"])
    @property
    def type(self) -> Literal["read_json"]:
        """Read-only public getter; included in .model_dump()/.model_dump_json()."""
        return self._type

    @classmethod
    def build_objects(cls, values: dict) -> dict:
        """Initialize layout, strategy, receiver, and metadata for the component."""
        values.setdefault("layout", Layout())
        values["strategy"] = get_strategy(values["strategy_type"])
        values["receiver"] = JSONReceiver()
        values.setdefault("metadata", MetaData())
        return values

    def process_row(self, row: Dict[str, Any], metrics: ComponentMetrics) -> Dict[str, Any]:
        """Read a single row from the JSON file."""
        return self.receiver.read_row(filepath=self.filepath, metrics=metrics)

    def process_bulk(self, data: List[Dict[str, Any]], metrics: ComponentMetrics) -> DataFrame:
        """Read all rows from the JSON file."""
        return self.receiver.read_bulk(filepath=self.filepath, metrics=metrics)

    def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics) -> Any:
        """Read rows from a large JSON file using a streaming approach."""
        return self.receiver.read_bigdata(filepath=self.filepath, metrics=metrics)