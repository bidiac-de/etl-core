from abc import ABC
from enum import Enum
from typing import List, Optional, Any
from datetime import datetime
from src.strategies.base_strategy import ExecutionStrategy
from src.receivers.base_receiver import Receiver
from src.metrics.base_metrics import Metrics
from src.components.dataclasses import MetaData, Layout
from pydantic import BaseModel, Field



class RuntimeState(Enum):
    """Runtime state of a component during execution."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"

class Component(ABC):
    def __init__(
            self,
            id: int,
            name: str,
            description: str,
            comp_type: str,
            strategy: Optional[ExecutionStrategy] = None,
            receiver: Optional[Receiver] = None,
            metrics: Optional[Metrics] = None,
            x_coord: float = 0.0,
            y_coord: float = 0.0,
            created_by: int = 0,
            created_at: datetime = datetime.now()
    ):
        self.id = id
        self.name = name
        self.description = description
        self.type = comp_type
        self.next_components = []
        self.prev_components = []
        self.status = RuntimeState.PENDING.value
        self.strategy = strategy
        self.receiver = receiver
        self.metrics = metrics
        self.layout = Layout(x_coord, y_coord)
        self.metadata = MetaData(created_at, created_by)

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

class BaseComponentSchema(BaseModel):
    id: str
    name: str
    description: str = ""
    # discriminator for component type
    type: str
    x_coord: float
    y_coord: float
    created_by: int = Field(..., description="ID of the user who created the component")
    created_at: datetime = Field(..., description="Timestamp when the component was created")