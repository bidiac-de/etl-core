# base class for data operations abstract classes hierrein
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from components.base import Component


class DataOperationsComponent(Component):
    """Concrete data operations component, e.g. filter, map, transform."""

    def __init__(self, *args, subcomponents: Optional[List[Component]] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.subcomponents = subcomponents or []

    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        pass

    def process_bulk(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pass

    def process_bigdata(self, chunk_iterable):
        pass