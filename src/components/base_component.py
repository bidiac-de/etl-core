from abc import ABC
from enum import Enum
from typing import Optional, List, Any
from datetime import datetime

from src.strategies.base_strategy import ExecutionStrategy
from src.receivers.base_receiver import Receiver
from src.components.dataclasses import MetaData, Layout
from pydantic import BaseModel, Field, ConfigDict


class RuntimeState(Enum):
    """
    Runtime state of a component during execution
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class Component(BaseModel, ABC):
    """
    Base class for all components in the system
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )
    id: int
    name: str
    description: str
    type: str
    layout: Layout = Field(default_factory=lambda: Layout(x_coord=0.0, y_coord=0.0))
    created_by: int
    created_at: datetime

    strategy: Optional[ExecutionStrategy] = Field(default=None, exclude=True)
    receiver: Optional[Receiver] = Field(default=None, exclude=True)
    next_components: List["Component"] = Field(default_factory=list, exclude=True)
    prev_components: List["Component"] = Field(default_factory=list, exclude=True)
    status: str = Field(default=RuntimeState.PENDING.value, exclude=True)
    metadata: MetaData = Field(
        default_factory=lambda: MetaData(datetime.now(), 0), exclude=True
    )
    metrics: Any = Field(default=None, exclude=True)

    def __init__(
        self,
        id: int,
        name: str,
        comp_type: str,
        description: str = "",
        strategy: Optional[ExecutionStrategy] = None,
        receiver: Optional[Receiver] = None,
        layout: Layout = Layout(x_coord=0.0, y_coord=0.0),
        created_by: int = 0,
        created_at: datetime = datetime.now(),
        **kwargs: Any,
    ):
        super().__init__(
            id=id,
            name=name,
            description=description,
            type=comp_type,
            layout=layout,
            created_by=created_by,
            created_at=created_at,
        )
        self.strategy = strategy
        self.receiver = receiver
        self.metadata = MetaData(created_at, created_by)

    def add_next(self, nxt: "Component"):
        """
        Add a next component to the current component
        :param nxt: The next component to add
        """
        self.next_components.append(nxt)

    def add_prev(self, prev: "Component"):
        """
        Add a previous component to the current component
        :param prev: The previous component to add
        """
        self.prev_components.append(prev)

    def execute(self, data, **kwargs) -> Any:
        """

        :param data: the data to be processed by the component
        :return: result of the component execution
        """
        if not self.strategy:
            raise ValueError(f"No strategy set for component {self.name}")
        return self.strategy.execute(self, data)
