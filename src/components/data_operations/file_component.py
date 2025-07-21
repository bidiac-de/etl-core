from pathlib import Path
from typing import List
from typing import Any, Dict, List
from components.base import Component

class ColumnDefinition:
    def __init__(self, name: str, dtype: str):
        self.name = name
        self.dtype = dtype

class FileComponent(Component):
    def __init__(self, filepath: Path, schemaDefinition: List[ColumnDefinition]):
        super().__init__(name="FileComponent", comp_type="file")
        self.filepath = filepath
        self.schemaDefinition = schemaDefinition

    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        pass

    def process_bulk(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pass

    def process_bigdata(self, chunk_iterable):
        pass