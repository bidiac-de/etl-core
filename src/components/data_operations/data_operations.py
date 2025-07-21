# base class for data operations abstract classes hierrein
import hashlib
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional
from components.base import Component


class DatabaseComponent(Component):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

class FileComponent(Component):
    @abstractmethod
    def read(self, filepath: str) -> Any:
        pass

    @abstractmethod
    def write(self, filepath: str, data: Any) -> None:
        pass

class DataOperationsComponent(Component):
    def __init__(self, *args, subcomponents: Optional[List[Component]] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.subcomponents = subcomponents or []

    @abstractmethod
    def execute(self, data: Any) -> Any:
        pass


class DataOperationsComponent(Component):
    def __init__(self):
        super().__init__(name="DataOperationsComponent", comp_type="data_operations")


    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
            pass

    def process_bulk(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pass

    def process_bigdata(self, chunk_iterable):
        pass