from abc import ABC, abstractmethod
from typing import Any, Dict, List
from pathlib import Path
from pydantic import Field, ConfigDict

from src.components.file_components.file_component import FileComponent
from src.components.column_definition import ColumnDefinition
from src.metrics.component_metrics import ComponentMetrics
from src.receivers.files.json_receiver import JSONReceiver


class JSON(FileComponent, ABC):
    """Abstract base class for JSON file components."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )

    filepath: Path = Field(..., description="Path to the JSON file")
    schema_definition: List[ColumnDefinition] = Field(..., description="Schema definition for JSON structure")
    metrics: ComponentMetrics = None
    receiver: JSONReceiver = None

    @abstractmethod
    def process_row(self, row: Dict[str, Any], metrics: ComponentMetrics) -> Dict[str, Any]:
        """
        Process a single JSON row (dict).
        """
        raise NotImplementedError

    @abstractmethod
    def process_bulk(self, data: List[Dict[str, Any]], metrics: ComponentMetrics) -> List[Dict[str, Any]]:
        """
        Process a list of JSON rows (dicts).
        """
        raise NotImplementedError

    @abstractmethod
    def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics) -> Any:
        """
        Process JSON data in a streaming/big data fashion.
        """
        raise NotImplementedError