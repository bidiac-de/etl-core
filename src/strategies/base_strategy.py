from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any
from pydantic import BaseModel, Field
from uuid import uuid4

if TYPE_CHECKING:
    from src.components.base_component import Component


class ExecutionStrategy(BaseModel, ABC):
    id: str = Field(default_factory=lambda: str(uuid4()), exclude=True)

    @abstractmethod
    def execute(self, component: "Component", inputs: Any) -> Any:
        """Execute the component with the given inputs"""
        raise NotImplementedError
