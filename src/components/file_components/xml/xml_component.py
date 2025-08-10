from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List
from pydantic import Field, ConfigDict

from src.components.file_components.file_component import FileComponent
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.components import Schema


class XMLTag(str, Enum):
    ROOT = "rows"
    RECORD = "row"


class XML(FileComponent):
    """Abstract base class for XML file components."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )

    root_tag: str = Field(default="rows")
    record_tag: str = Field(default="row")

    @abstractmethod
    async def process_row(self, row: Dict[str, Any], metrics: ComponentMetrics) -> Dict[str, Any]:
        """
        Process a single record.
        """
        raise NotImplementedError

    @abstractmethod
    async def process_bulk(self, data: List[Dict[str, Any]], metrics: ComponentMetrics) -> List[Dict[str, Any]]:
        """
        Process multiple records.
        """
        raise NotImplementedError

    @abstractmethod
    async def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics) -> Any:
        """
        Process big data / streaming.
        """
        raise NotImplementedError