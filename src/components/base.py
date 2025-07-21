import hashlib
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional

class ComponentManager:
    pass

class ComponentRuleSet:
    pass

class Receiver:
    pass

class Metrics:
    pass

class Layout:
    pass

class MetaData:
    pass

class RuntimeState(Enum):
    """Runtime state of a component during execution."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"

class Component(ABC):
    def __init__(
            self,
            id: int,
            name: str,
            description: str,
            comp_type: str,
            componentManager: Optional[ComponentManager] = None,
            componentRuleSet: Optional[ComponentRuleSet] = None,
            next_components: Optional[List["Component"]] = None,
            prev_components: Optional[List["Component"]] = None,
            status: RuntimeState = RuntimeState.PENDING,
            strategy: Optional[ExecutionStrategy] = None,
            receiver: Optional[Receiver] = None,
            metrics: Optional[Metrics] = None,
            layout: Optional[Layout] = None,
            metadata: Optional[MetaData] = None
    ):
        self.id = id
        self.name = name
        self.description = description
        self.type = comp_type
        self.componentManager = componentManager
        self.componentRuleSet = componentRuleSet
        self.next_components = next_components or []
        self.prev_components = prev_components or []
        self.status = status
        self.strategy = strategy
        self.receiver = receiver
        self.metrics = metrics
        self.layout = layout
        self.metadata = metadata

    def add_next(self, nxt: "Component"):
        self.next_components.append(nxt)
        nxt.prev_components.append(self)

    def add_prev(self, prev: "Component"):
        self.prev_components.append(prev)
        prev.next_components.append(self)

    def execute(self, data, **kwargs) -> Any:
        if not self.strategy:
            raise ValueError(f"No strategy set for component {self.name}")
        return self.strategy.execute(self, data)

    @abstractmethod
    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        pass

    @abstractmethod
    def process_bulk(self, data):
        pass

    @abstractmethod
    def process_bigdata(self, chunk_iterable):
        pass

    def save(self):
        # Persist logic
        pass

    def load(self):
        # Load logic
        pass

