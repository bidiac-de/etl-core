# read_json_component.py
import json
from pathlib import Path
from typing import Any, Dict, List
from src.components.file_components.json.json_component import JSON
from src.receivers.files.json_receiver import JSONReceiver
from src.components.column_definition import ColumnDefinition


class ReadJSON(JSON):
    """Component that reads data from a JSON file."""

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
        return self.receiver.read_row()

    def process_bulk(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return self.receiver.read_bulk()

    def process_bigdata(self, chunk_iterable):
        return self.receiver.read_bigdata()