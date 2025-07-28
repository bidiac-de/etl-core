from abc import ABC
from enum import Enum
from typing import Optional, List, Any
from datetime import datetime
from abc import abstractmethod
from uuid import uuid4

from src.strategies.base_strategy import ExecutionStrategy
from src.receivers.base_receiver import Receiver
from src.components.dataclasses import MetaData, Layout
from pydantic import BaseModel, Field, ConfigDict, model_validator
from src.strategies.row_strategy import RowExecutionStrategy
from src.strategies.bulk_strategy import BulkExecutionStrategy
from src.strategies.bigdata_strategy import BigDataExecutionStrategy


class RuntimeState(Enum):
    """
    Runtime state of a component during execution
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class StrategyType(Enum):
    """
    Enum for different strategy types
    """

    ROW = "row"
    BULK = "bulk"
    BIGDATA = "bigdata"


class Component(BaseModel, ABC):
    """
    Base class for all components in the system
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )
    id: str = Field(default_factory=lambda: str(uuid4()), exclude=True)
    temp_id: int  # also has to be unique, responsibility of the caller to ensure
    name: str
    description: str
    comp_type: str
    created_by: int
    created_at: datetime
    x_coord: float = Field(default=0.0)
    y_coord: float = Field(default=0.0)
    strategy_type: StrategyType = Field(default=StrategyType.ROW.value)
    next: Optional[List[int]] = None

    next_components: List["Component"] = Field(default_factory=list, exclude=True)
    prev_components: List["Component"] = Field(default_factory=list, exclude=True)
    status: str = Field(default=RuntimeState.PENDING.value, exclude=True)
    metrics: Any = Field(default=None, exclude=True)

    # these need to be created in the concrete component classes
    strategy: Optional[ExecutionStrategy] = Field(default=None, exclude=True)
    receiver: Optional[Receiver] = Field(default=None, exclude=True)
    layout: Optional[Layout] = Field(default=None, exclude=True)
    metadata: MetaData = Field(default_factory=lambda: MetaData(), exclude=True)

    @model_validator(mode="before")
    @classmethod
    @abstractmethod
    def build_objects(cls, values: dict) -> dict:
        """
        Each concrete component must implement this method to:
        - construct strategy, receiver, layout, and metadata
        - modify and return the values dict
        """
        raise NotImplementedError

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


def get_strategy(strategy_type: str) -> ExecutionStrategy:
    """
    Factory function to get the appropriate execution strategy based on the type.
    """
    if strategy_type == "row":
        return RowExecutionStrategy()
    elif strategy_type == "bulk":
        return BulkExecutionStrategy()
    elif strategy_type == "bigdata":
        return BigDataExecutionStrategy()
    else:
        raise ValueError(f"Unknown strategy type: {strategy_type}")
