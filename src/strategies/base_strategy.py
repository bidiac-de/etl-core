from abc import ABC, abstractmethod
from typing import Any
#from src.components.base_components import Component

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from src.components.base_components import Component

class ExecutionStrategy(ABC):
    @abstractmethod
    def execute(self, component: "Component", inputs: Any) -> Any:
        """Execute the component with the given inputs"""
        raise NotImplementedError
