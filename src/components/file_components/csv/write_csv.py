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


@register_component("write_csv")
class WriteCSV(CSV):
    """Component that writes data to a CSV file."""

    type: Literal["write_csv"] = Field(default="write_csv")
    filepath: Path = Field(..., description="Path to the CSV file")
    separator: Delimiter = Field(default=Delimiter.COMMA, description="CSV field separator")
    schema_definition: List[ColumnDefinition] = Field(..., description="Schema definition for CSV columns")

    metrics: ComponentMetrics = Field(
        default_factory=lambda: ComponentMetrics(started_at=None, processing_time=0)
    )
    receiver: CSVReceiver = None

    @classmethod
    def build_objects(cls, values):
        """Initialize layout, strategy, receiver, and metadata for the component."""
        values.setdefault("layout", Layout())
        values["strategy"] = get_strategy(values["strategy_type"])
        values["receiver"] = CSVReceiver(filepath=values["filepath"])
        values.setdefault("metadata", MetaData())
        values.setdefault("metrics", ComponentMetrics(started_at=None, processing_time=0))
        return values

    def process_row(self, row: Dict[str, Any], metrics: ComponentMetrics) -> Dict[str, Any]:
        """Write a single row to the CSV file."""
        self.receiver.write_row(row=row, filepath=self.filepath, metrics=metrics)
        return row

    def process_bulk(self, data: List[Dict[str, Any]], metrics: ComponentMetrics) -> List[Dict[str, Any]]:
        """Write multiple rows to the CSV file."""
        self.receiver.write_bulk(data=data, filepath=self.filepath, metrics=metrics)
        return data

    def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics) -> Any:
        """Write large amounts of data to the CSV file using a streaming approach."""
        self.receiver.write_bigdata(data=chunk_iterable, filepath=self.filepath, metrics=metrics)
        return chunk_iterable