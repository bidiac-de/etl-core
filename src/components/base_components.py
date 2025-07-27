from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional

from src.strategies.base_strategy import ExecutionStrategy
from src.receivers.receiver import Receiver
from src.context.context_provider import IContextProvider

from src.metrics.component_metrics import ComponentMetrics
from src.components.dataclasses import Layout, MetaData


class RuntimeState(Enum):
    """Defines the runtime status of a component."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class Component(ABC):
    """
    Abstract base class for all components used in the ETL pipeline.
    Components can define execution strategies and connect to other components.
    """

    def __init__(
        self,
        id: int,
        name: str,
        description: str,
        type: str,
        strategy: Optional[ExecutionStrategy] = None,
        receiver: Optional[Receiver] = None,
        layout: Optional[Layout] = None,
        metadata: Optional[MetaData] = None,
        contexts: Optional[List[IContextProvider]] = None,

    ):
        self.id = id
        self.name = name
        self.description = description
        self.type = type
        self.status = RuntimeState.PENDING.value
        self.strategy = strategy
        self.receiver = receiver
        self.metrics = None
        self.layout = layout
        self.metadata = metadata
        self.prev_components: List["Component"] = []
        self.next_components: List["Component"] = []
        self.contexts: List[IContextProvider] = contexts or []

    def add_next(self, nxt: "Component"):
        """Connects this component to its successor."""
        self.next_components.append(nxt)


    def add_prev(self, prev: "Component"):
        """Connects this component to its predecessor."""
        self.prev_components.append(prev)


    def execute(self, data: Any, **kwargs) -> Any:
        """
        Executes the component using its strategy.
        Raises ValueError if no strategy is defined.
        """
        if not self.strategy:
            raise ValueError(f"No strategy defined for component '{self.name}'")
        return self.strategy.execute(self, data)

    @abstractmethod
    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Processes a single row of data.
        Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def process_bulk(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Processes data in bulk mode.
        Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def process_bigdata(self, chunk_iterable: Any) -> Any:
        """
        Processes large data chunks (e.g., generators or streams).
        Must be implemented by subclasses.
        """
        pass
