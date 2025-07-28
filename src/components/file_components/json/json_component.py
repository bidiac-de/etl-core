from abc import ABC, abstractmethod
from src.components.file_components.file_component import FileComponent
from typing import Any, Dict, List
from pathlib import Path
from src.components.column_definition import ColumnDefinition


class JSON(FileComponent, ABC):
    """Abstract base class for JSON file components."""

    def __init__(self,
                 id: int,
                 name: str,
                 description: str,
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

    @abstractmethod
    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        pass

    @abstractmethod
    def process_bulk(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def process_bigdata(self, chunk_iterable):
        pass
