from abc import ABC, abstractmethod
from src.components.file_components.file_component import FileComponent
from typing import Any, Dict, List
from pathlib import Path
from src.components.column_definition import ColumnDefinition
from enum import Enum


class Delimiter(Enum):
    COMMA = ','
    SEMICOLON = ';'
    TAB = '\t'


class CSV(FileComponent, ABC):
    """Abstract base class for CSV file components."""

    def __init__(self,
                 id: int,
                 name: str,
                 description: str,
                 componentManager,
                 filepath: Path,
                 schema_definition: List[ColumnDefinition],
                 separator: Delimiter = Delimiter.COMMA
                 ):
        super().__init__(
            id,
            name,
            description,
            componentManager,
            filepath,
            schema_definition
        )
        self.separator = separator

    @abstractmethod
    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single row of CSV data."""
        pass

    @abstractmethod
    def process_bulk(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process multiple rows of CSV data."""
        pass

    @abstractmethod
    def process_bigdata(self, chunk_iterable):
        """Process CSV data in a streaming/big data fashion."""
        pass