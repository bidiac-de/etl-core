from pathlib import Path
from typing import List, Dict, Any
from components.base import Component
from src.components.column_definition import ColumnDefinition


class FileComponent(Component):
    """Abstract base class for file-based components like CSV, JSON, usw."""
    def __init__(self,
                 id: int,
                 name: str,
                 description: str,
                 componentManager,
                 filepath: Path,
                 schema_definition: List[ColumnDefinition]):
        super().__init__(
            id=id,
            name=name,
            description=description,
            comp_type="file",
            componentManager=componentManager
        )
        self.filepath = filepath
        self.schema_definition = schema_definition

    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        pass

    def process_bulk(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pass

    def process_bigdata(self, chunk_iterable):
        pass