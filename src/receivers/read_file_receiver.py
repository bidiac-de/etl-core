from abc import ABC, abstractmethod
from typing import Dict, Any, List, Generator
from pathlib import Path
from src.receivers.base_receiver import Receiver
from src.metrics.component_metrics import ComponentMetrics


class ReadFileReceiver(Receiver, ABC):
    """Abstract receiver for reading data."""

    def __init__(self, id: int = 0):
        super().__init__(id)

    @abstractmethod
    def read_row(self, filepath: Path, metrics: ComponentMetrics) -> Dict[str, Any]:
        """Reads a single row."""
        pass

    @abstractmethod
    def read_bulk(self, filepath: Path, metrics: ComponentMetrics) -> List[Dict[str, Any]]:
        """Reads multiple rows as list."""
        pass

    @abstractmethod
    def read_bigdata(self, filepath: Path, metrics: ComponentMetrics) -> Generator[Dict[str, Any], None, None]:
        """Reads big data in a generator/streaming manner."""
        pass