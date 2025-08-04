from abc import ABC, abstractmethod
from typing import Any, AsyncIterator
from enum import Enum

from pydantic import BaseModel
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.components.base_component import Component


class ExecutionStrategy(BaseModel, ABC):  # noqa: WPS214
    """
    Base class for streaming execution strategies.
    Subclasses implement `execute` as an async generator to drive streaming.
    """

    @abstractmethod
    def execute(
        self,
        component: Component,
        payload: Any,
        metrics: ComponentMetrics,
    ) -> AsyncIterator[Any]:
        """
        Stream through the component logic, yielding native outputs.
        """


class StrategyType(str, Enum):
    """
    Enum for different strategy types
    """

    ROW = "row"
    BULK = "bulk"
    BIGDATA = "bigdata"
