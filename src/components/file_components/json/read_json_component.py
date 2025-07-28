# read_json_component.py
import json
from pathlib import Path
from typing import Any, Dict, List
from src.components.file_components.json.json_component import JSON
from src.receivers.files.json_receiver import JSONReceiver
from src.components.column_definition import ColumnDefinition
from src.components.registry import register_component

@register_component("read_json")
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


    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return JSONReceiver().read_row(filepath=self.filepath)

    def process_bulk(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return JSONReceiver().read_bulk(filepath=self.filepath)

    def process_bigdata(self, chunk_iterable):
        JSONReceiver().read_bigdata(filepath=self.filepath)