from pathlib import Path
from typing import Any, Dict, List, Generator, Literal
from pydantic import Field

from src.components.file_components.csv.csv_component import CSV, Delimiter
from src.components.column_definition import ColumnDefinition
from src.components.dataclasses import Layout, MetaData
from src.components.base_components import get_strategy
from src.components.registry import register_component
from src.receivers.files.csv_receiver import CSVReceiver
from src.metrics.component_metrics import ComponentMetrics


@register_component("read_csv")
class ReadCSV(CSV):
    """Component that reads data from a CSV file."""

    type: Literal["read_csv"] = Field(default="read_csv")


    @classmethod
    def build_objects(cls, values):
        """Initialize layout, strategy, receiver, and metadata for the component."""
        values.setdefault("layout", Layout())

        values["strategy"] = get_strategy(values["strategy_type"])
        values["receiver"] = CSVReceiver(filepath=values["filepath"])
        values.setdefault("metadata", MetaData())
        return values

    def process_row(self, row: Dict[str, Any], metrics: ComponentMetrics) -> Dict[str, Any]:
        """ Read a single row from the CSV file. """
        return self.receiver.read_row(metrics=metrics)

    def process_bulk(self, data: List[Dict[str, Any]], metrics: ComponentMetrics) -> List[Dict[str, Any]]:
        """ Read all rows from the CSV file. """
        return self.receiver.read_bulk(metrics=metrics)

    def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics) -> Any:
        """ Read rows from a large CSV file using a streaming approach. """
        return self.receiver.read_bigdata(metrics=metrics)