from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional
from pathlib import Path
from enum import Enum
from pydantic import Field, ConfigDict
from src.components.file_components.file_component import FileComponent
from src.components.column_definition import ColumnDefinition


class Delimiter(str, Enum):
    """Enum for possible CSV delimiters."""
    COMMA = ','
    SEMICOLON = ';'
    TAB = '\t'


class CSV(FileComponent, ABC):
    """Abstract base class for CSV file components."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )

    id: int
    name: str
    description: str
    comp_type: str
    created_by: int
    created_at: datetime
    filepath: Path = Field(..., description="Path to the CSV file")
    schema_definition: List[ColumnDefinition] = Field(..., description="Schema definition for CSV columns")
    separator: Delimiter = Field(default=Delimiter.COMMA, description="CSV field separator")

    @abstractmethod
    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single row of CSV data."""
        raise NotImplementedError

    @abstractmethod
    def process_bulk(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process multiple rows of CSV data."""
        raise NotImplementedError

    @abstractmethod
    def process_bigdata(self, chunk_iterable):
        """Process CSV data in a streaming/big data fashion."""
        raise NotImplementedError