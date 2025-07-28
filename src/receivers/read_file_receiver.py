# src/receivers/read_file_receiver.py
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Generator
from pathlib import Path
from src.receivers.receiver import Receiver


class ReadFileReceiver(Receiver, ABC):
    """Abstract receiver for reading data."""

    def __init__(self, id: int = 0):
        super().__init__(id)

    @abstractmethod
    def read_row(self, filepath: Path) -> Dict[str, Any]:
        """Reads a single row."""
        pass

    @abstractmethod
    def read_bulk(self, filepath: Path) -> List[Dict[str, Any]]:
        """Reads multiple rows as list."""
        pass

    @abstractmethod
    def read_bigdata(self, filepath: Path) -> Generator[Dict[str, Any], None, None]:
        """Reads big data in a generator/streaming manner."""
        pass