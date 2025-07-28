from pathlib import Path
from typing import Any, Dict, List, Generator, Literal
from pydantic import Field
from datetime import datetime
from src.components.file_components.csv.csv_component import CSV, Delimiter
from src.components.column_definition import ColumnDefinition
from src.components.dataclasses import Layout, MetaData
from src.components.base_components import get_strategy
from src.components.registry import register_component
from receivers.files.csv_receiver import CSVReceiver
from src.metrics.component_metrics import ComponentMetrics


@register_component("write_csv")
class WriteCSV(CSV):
    """Component that writes data to a CSV file."""

    type: Literal["write_csv"]
    filepath: Path = Field(..., description="Path to the CSV file")
    separator: Delimiter = Field(default=Delimiter.COMMA, description="CSV field separator")
    schema_definition: List[ColumnDefinition] = Field(..., description="Schema definition for CSV columns")

    metrics: ComponentMetrics = None

    @classmethod
    def build_objects(cls, values):
        values["layout"] = Layout(
            x_coord=values["x_coord"],
            y_coord=values["y_coord"]
        )
        values["strategy"] = get_strategy(values["strategy_type"])
        values["receiver"] = CSVReceiver(
            filepath=values["filepath"]
        )
        values["metadata"] = MetaData(
            created_at=values["created_at"],
            created_by=values["created_by"]
        )
        return values

    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        self.metrics = ComponentMetrics(started_at=datetime.now(), processing_time=None)
        try:
            self.receiver.write_row(row)
            self.metrics.lines_received = 1
            self.metrics.lines_forwarded = 1
            self.metrics.processing_time = datetime.now() - self.metrics.started_at
            return row
        except Exception:
            self.metrics.error_count += 1
            self.metrics.processing_time = datetime.now() - self.metrics.started_at
            raise

    def process_bulk(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        self.metrics = ComponentMetrics(started_at=datetime.now(), processing_time=None)
        try:
            self.receiver.write_bulk(data)
            self.metrics.lines_received = len(data)
            self.metrics.lines_forwarded = len(data)
            self.metrics.processing_time = datetime.now() - self.metrics.started_at
            return data
        except Exception:
            self.metrics.error_count += 1
            self.metrics.processing_time = datetime.now() - self.metrics.started_at
            raise

    def process_bigdata(self, chunk_iterable: Generator[Dict[str, Any], None, None]):
        self.metrics = ComponentMetrics(started_at=datetime.now(), processing_time=None)
        count = 0
        try:
            self.receiver.write_bigdata(chunk_iterable)
            for _ in chunk_iterable:
                count += 1
            self.metrics.lines_received = count
            self.metrics.lines_forwarded = count
            self.metrics.processing_time = datetime.now() - self.metrics.started_at
            return chunk_iterable
        except Exception:
            self.metrics.error_count += 1
            self.metrics.processing_time = datetime.now() - self.metrics.started_at
            raise