# src/components/file_components/xml/xml_component.py
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional
from pydantic import Field, ConfigDict

from src.components.file_components.file_component import FileComponent
from src.components.column_definition import ColumnDefinition
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.files.xml_receiver import XMLReceiver

class XML(FileComponent, ABC):
    """Abstract base class for XML file components."""
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

    filepath: Path = Field(..., description="Path to the XML file")
    schema_definition: List[ColumnDefinition] = Field(..., description="Schema definition for XML structure")
    metrics: Optional[ComponentMetrics] = None
    receiver: Optional[XMLReceiver] = None

    @abstractmethod
    def process_row(self, row: Dict[str, Any], metrics: ComponentMetrics = None) -> Dict[str, Any]:
        """
       Process a single XML row (dict).
       """
        raise NotImplementedError

    @abstractmethod
    def process_bulk(self, data: List[Dict[str, Any]], metrics: ComponentMetrics = None):
        """
        Process a list of XML rows (dicts).
        """
        raise NotImplementedError

    @abstractmethod
    def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics = None):
        """
        Process XML data in a streaming/big data fashion.
        """
        raise NotImplementedError
