from typing import List, Optional
from etl_core.components.base_component import Component
from abc import ABC


class DataOperationsComponent(Component, ABC):
    """Concrete data operations component, e.g. filter, map, transform."""

    def __init__(
        self, *args, subcomponents: Optional[List[Component]] = None, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.subcomponents = subcomponents or []
