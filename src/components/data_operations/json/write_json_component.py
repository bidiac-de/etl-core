# write_json_component.py
import json
from pathlib import Path
from typing import Any, Dict, List
from src.components.data_operations.json.json_component import JSON
from src.receivers.json_receiver import JSONReceiver
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
        self.receiver = JSONReceiver(filepath)

    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        self.receiver.write_row(row)
        return row

    def process_bulk(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        self.receiver.write_bulk(data)
        return data

    def process_bigdata(self, chunk_iterable):
        self.receiver.write_bigdata(chunk_iterable)
        return chunk_iterable

