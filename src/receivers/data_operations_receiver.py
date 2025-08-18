from abc import ABC, abstractmethod
from typing import Dict, Any, List, Generator
from src.receivers.base_receiver import Receiver


class DataOperationsReceiver(Receiver, ABC):
    """Abstract base class for receivers that process data."""

    def __init__(self, id: int = 0):
        super().__init__(id)

    @abstractmethod
    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Processes a single row."""
        pass

    @abstractmethod
    def process_bulk(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Processes multiple rows."""
        pass

    @abstractmethod
    def process_bigdata(self, chunk_iterable: Generator[Dict[str, Any], None, None]):
        """Processes streaming/big data."""
        pass
