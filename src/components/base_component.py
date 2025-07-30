from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, List, Any, Dict
from datetime import datetime
from uuid import uuid4
from pydantic import BaseModel, Field, ConfigDict, model_validator

from src.components.dataclasses import MetaData, Layout
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.base_receiver import Receiver
from src.strategies.base_strategy import ExecutionStrategy
from src.strategies.bigdata_strategy import BigDataExecutionStrategy
from src.strategies.bulk_strategy import BulkExecutionStrategy
from src.strategies.row_strategy import RowExecutionStrategy


class StrategyType(str , Enum):
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
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    description: str
    comp_type: str
    strategy_type: StrategyType = Field(default=StrategyType.ROW.value)
    next: [List[str]] = []
    layout: [Layout] = Field(default_factory=lambda: Layout())
    metadata: MetaData = Field(default_factory=lambda: MetaData())

    next_components: List["Component"] = Field(default_factory=list, exclude=True)
    prev_components: List["Component"] = Field(default_factory=list, exclude=True)
    metrics: Any = Field(default=None, exclude=True)

    # these need to be created in the concrete component classes
    strategy: Optional[ExecutionStrategy] = Field(default=None, exclude=True)
    receiver: Optional[Receiver] = Field(default=None, exclude=True)

    @model_validator(mode="before")
    @classmethod
    @abstractmethod
    def build_objects(cls, values: dict) -> dict:
        """
        Each concrete component must implement this method to:
        - construct strategy and receiver
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

    def execute(self, data, metrics: ComponentMetrics, **kwargs) -> Any:
        """

        :param data: the data to be processed by the component
        :param metrics: a ComponentMetrics instance constructed by the execution handler
        :return: result of the component execution
        """
        if not self.strategy:
            raise ValueError(f"No strategy set for component {self.name}")
        return self.strategy.execute(self, data, metrics)

    @abstractmethod
    def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def process_bulk(
        self, data: List[Dict[str, Any]], metrics: ComponentMetrics
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics) -> Any:
        raise NotImplementedError


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
