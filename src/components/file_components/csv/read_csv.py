from pathlib import Path
from typing import Any, Dict, List, Generator, Literal
from pydantic import Field
from src.components.file_components.csv.csv_component import CSV, Delimiter
from src.components.column_definition import ColumnDefinition
from src.components.dataclasses import Layout, MetaData
from src.components.base_components import get_strategy
from src.components.registry import register_component
from receivers.files.csv_receiver import CSVReceiver


@register_component("read_csv")
class ReadCSV(CSV):
    """Component that reads data from a CSV file."""

    type: Literal["read_csv"]
    filepath: Path = Field(..., description="Path to the CSV file")
    separator: Delimiter = Field(default=Delimiter.COMMA, description="CSV field separator")
    schema_definition: List[ColumnDefinition] = Field(..., description="Schema definition for CSV columns")

    @classmethod
    def build_objects(cls, values):
        """
        Build dependent objects for the CSV read component
        """
        values["layout"] = Layout(
            x_coord=values["x_coord"],
            y_coord=values["y_coord"]
        )
        values["strategy"] = get_strategy(values["strategy_type"])
        values["receiver"] = CSVReceiver(
            filepath=values["filepath"],
            separator=values.get("separator", Delimiter.COMMA)
        )
        values["metadata"] = MetaData(
            created_at=values["created_at"],
            created_by=values["created_by"]
        )
        return values

    def process_row(self, row: Dict[str, Any] = None) -> Dict[str, Any]:
        return self.receiver.read_row()

    def process_bulk(self, data: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        return self.receiver.read_bulk()

    def process_bigdata(self, chunk_iterable=None) -> Generator[Dict[str, Any], None, None]:
        return self.receiver.read_bigdata()