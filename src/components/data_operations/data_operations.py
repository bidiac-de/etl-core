from typing import List, Optional
from src.components.base_components import Component


class DataOperationsComponent(Component):
    """Concrete data operations component, e.g. filter, map, transform."""

    def __init__(
        self, *args, subcomponents: Optional[List[Component]] = None, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.subcomponents = subcomponents or []
