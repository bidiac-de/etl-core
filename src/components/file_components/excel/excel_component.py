from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List
from pydantic import Field, ConfigDict

from src.components.file_components.file_component import FileComponent
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.files.excel_receiver import ExcelReceiver
from src.components import Schema


class Excel(FileComponent, ABC):
    """Abstract base class for Excel file components."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )

    schema: Schema = Field(..., description="Schema definition")
    receiver: ExcelReceiver = None

    @abstractmethod
    def process_row(self, row: Dict[str, Any], metrics: ComponentMetrics) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def process_bulk(self, data: List[Dict[str, Any]], metrics: ComponentMetrics) -> List[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics) -> Any:
        raise NotImplementedError