from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, TYPE_CHECKING
from src.components.envelopes import Out

from pydantic import BaseModel
from src.metrics.component_metrics.component_metrics import ComponentMetrics

if TYPE_CHECKING:
    from src.components.base_component import Component


class ExecutionStrategy(BaseModel, ABC):
    """
    Base class for streaming execution strategies.
    Subclasses implement `execute` as an async generator to drive streaming.
    """

    @abstractmethod
    async def execute(
        self,
        component: "Component",
        payload: Any,
        metrics: ComponentMetrics,
    ) -> AsyncIterator[Out]:  # <-- align with subclasses
        """
        Stream through the component logic, yielding native outputs.
        """
        raise NotImplementedError
