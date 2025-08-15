from abc import ABC, abstractmethod
from typing import Dict, Any, List, Generator
from src.receivers.base_receiver import Receiver


class WriteDatabaseReceiver(Receiver, ABC):
    """Abstract receiver for writing data."""

    def __init__(self, id: int = 0):
        super().__init__(id)

    @abstractmethod
    def write_row(self, row: Dict[str, Any]):
        """Writes a single row."""
        pass

    @abstractmethod
    def write_bulk(self, data: List[Dict[str, Any]]):
        """Writes bulk data as list of rows."""
        pass

    @abstractmethod
    def write_bigdata(self, chunk_iterable: Generator[Dict[str, Any], None, None]):
        """Writes data in chunks (e.g. generator)."""
        pass
