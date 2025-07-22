from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from src.components.base_component import Component


class ExecutionStrategy(ABC):
    @abstractmethod
    def execute(self, component: "Component", inputs: Any) -> Any:
        """Execute the component with the given inputs"""
        raise NotImplementedError
