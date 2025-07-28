# src/receivers/write_file_receiver.py
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Generator
from pathlib import Path
from src.receivers.receiver import Receiver


class WriteFileReceiver(Receiver, ABC):
    """Abstract receiver for writing data."""

    def __init__(self, id: int = 0):
        super().__init__(id)

    @abstractmethod
    def write_row(self, row: Dict[str, Any], filepath: Path):
        """Writes a single row."""
        pass

    @abstractmethod
    def write_bulk(self, data: List[Dict[str, Any]], filepath: Path):
        """Writes bulk data as list of rows."""
        pass

    @abstractmethod
    def write_bigdata(self, chunk_iterable: Generator[Dict[str, Any], None, None], filepath: Path):
        """Writes data in chunks (e.g. generator)."""
        pass