# write_json_component.py
from pathlib import Path
from typing import Any, Dict, List
from src.components.file_components.json.json_component import JSON
from receivers.files.json_receiver import JSONReceiver
from src.components.column_definition import ColumnDefinition


class WriteJSON(JSON):
    """Component that writes data to a JSON file."""

    def __init__(
            self,
            id,
            name,
            description,
            componentManager,
            filepath: Path,
            schema_definition: List[ColumnDefinition]
    ):
        super().__init__(
            id,
            name,
            description,
            componentManager,
            filepath,
            schema_definition
        )

    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        JSONReceiver().write_row(row, filepath=self.filepath)
        return row

    def process_bulk(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        JSONReceiver().write_bulk(data, filepath=self.filepath)
        return data

    def process_bigdata(self, chunk_iterable):
        JSONReceiver().write_bigdata(chunk_iterable, filepath=self.filepath)
        return chunk_iterable

