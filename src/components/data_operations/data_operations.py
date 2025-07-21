# base class for data operations abstract classes hierrein
import hashlib
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional
from ..base import Component


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
