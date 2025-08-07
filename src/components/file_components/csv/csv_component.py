from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List
from pydantic import Field, ConfigDict
from src.components.file_components.file_component import FileComponent
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.components import Schema


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

    separator: Delimiter = Field(default=Delimiter.COMMA, description="CSV field separator")
    schema: Schema = Field(..., description="Schema definition")

    @abstractmethod
    async def process_row(self, row: Dict[str, Any], metrics: ComponentMetrics) -> Dict[str, Any]:
        """
        Process a single row of CSV data.
        """
        raise NotImplementedError

    @abstractmethod
    async def process_bulk(self, data: List[Dict[str, Any]], metrics: ComponentMetrics) -> List[Dict[str, Any]]:
        """
        Process multiple rows of CSV data.
        """
        raise NotImplementedError

    @abstractmethod
    async def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics) -> Any:
        """
        Process CSV data in a streaming/big data fashion.
        """
        raise NotImplementedError