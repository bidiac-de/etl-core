from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, List, Any, Dict
from datetime import datetime

from pydantic import BaseModel, Field, ConfigDict, model_validator

from src.strategies.base_strategy import ExecutionStrategy
from src.receivers.base_receiver import Receiver
from src.components.dataclasses import MetaData, Layout
from src.strategies.row_strategy import RowExecutionStrategy
from src.strategies.bulk_strategy import BulkExecutionStrategy
from src.strategies.bigdata_strategy import BigDataExecutionStrategy


class RuntimeState(Enum):
    """Runtime state of a component during execution"""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class StrategyType(str, Enum):
    """Enum for different strategy types"""
    ROW = "row"
    BULK = "bulk"
    BIGDATA = "bigdata"


class Component(BaseModel, ABC):
    """
    Base class for all components in the system.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )

    id: int
    name: str
    description: str
    comp_type: str
    created_by: int
    created_at: datetime
    x_coord: float = Field(default=0.0)
    y_coord: float = Field(default=0.0)
    strategy_type: StrategyType = Field(default=StrategyType.ROW)
    next: Optional[List[int]] = None

    next_components: List["Component"] = Field(default_factory=list, exclude=True)
    prev_components: List["Component"] = Field(default_factory=list, exclude=True)
    status: str = Field(default=RuntimeState.PENDING.value, exclude=True)
    metrics: Any = Field(default=None, exclude=True)

    strategy: Optional[ExecutionStrategy] = Field(default=None, exclude=True)
    receiver: Optional[Receiver] = Field(default=None, exclude=True)
    layout: Optional[Layout] = Field(default=None, exclude=True)
    metadata: MetaData = Field(default_factory=MetaData, exclude=True)

    @model_validator(mode="before")
    @classmethod
    def build_objects(cls, values: dict) -> dict:
        """Default factory for layout, strategy and metadata"""
        values.setdefault("layout", Layout(
            x_coord=values.get("x_coord", 0.0),
            y_coord=values.get("y_coord", 0.0),
        ))
        values.setdefault("strategy", get_strategy(str(values.get("strategy_type", "row"))))
        values.setdefault("metadata", MetaData(
            created_at=values.get("created_at", datetime.now()),
            created_by=values.get("created_by", 0),
        ))
        return values

    def add_next(self, nxt: "Component"):
        self.next_components.append(nxt)

    def add_prev(self, prev: "Component"):
        self.prev_components.append(prev)

    def execute(self, data, **kwargs) -> Any:
        if not self.strategy:
            raise ValueError(f"No strategy set for component {self.name}")
        return self.strategy.execute(self, data)

    @abstractmethod
    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def process_bulk(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def process_bigdata(self, chunk_iterable: Any) -> Any:
        raise NotImplementedError


def get_strategy(strategy_type: str) -> ExecutionStrategy:
    """Factory function to get the appropriate execution strategy based on the type."""
    if strategy_type == "row":
        return RowExecutionStrategy()
    elif strategy_type == "bulk":
        return BulkExecutionStrategy()
    elif strategy_type == "bigdata":
        return BigDataExecutionStrategy()
    else:
        raise ValueError(f"Unknown strategy type: {strategy_type}")