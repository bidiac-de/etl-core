from abc import ABC, abstractmethod
from typing import Optional, List, Any, Dict, AsyncIterator
from uuid import uuid4
from pydantic import (
    Field,
    ConfigDict,
    model_validator,
    PrivateAttr,
    field_validator,
)
from enum import Enum

from src.components.dataclasses import MetaData, Layout
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.base_receiver import Receiver
from src.strategies.base_strategy import ExecutionStrategy
from src.strategies.bigdata_strategy import BigDataExecutionStrategy
from src.strategies.bulk_strategy import BulkExecutionStrategy
from src.strategies.row_strategy import RowExecutionStrategy
from src.persistance.base_models.component_base import ComponentBase
from pandas import DataFrame


class StrategyType(str, Enum):
    """
    Enum for different strategy types
    """

    ROW = "row"
    BULK = "bulk"
    BIGDATA = "bigdata"


class Component(ComponentBase, ABC):
    """
    Base class for all components in the system
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
        validate_assignment=True,
    )
    _id: str = PrivateAttr(default_factory=lambda: str(uuid4()))
    next: List[str] = Field(default_factory=list)  # List of names of next components
    layout: Layout = Field(default_factory=lambda: Layout())
    metadata_: MetaData = Field(default_factory=lambda: MetaData(), alias="metadata")

    _next_components: List["Component"] = PrivateAttr(default_factory=list)
    _prev_components: List["Component"] = PrivateAttr(default_factory=list)

    # these need to be created in the concrete component classes
    _strategy: Optional[ExecutionStrategy] = PrivateAttr(default=None)
    _receiver: Optional[Receiver] = PrivateAttr(default=None)

    @model_validator(mode="after")
    @abstractmethod
    def _build_objects(self) -> "Component":
        """
        After-instantiation hook. Override in subclasses to assign
        `self._receiver`, then return `self`.
        """
        return self

    @field_validator("name", "comp_type", mode="before")
    @classmethod
    def _validate_non_empty_string(cls, value: str) -> str:
        """
        Validate that the name, comp_type, and strategy_type are non-empty strings.
        """
        if not isinstance(value, str) or not value.strip():
            raise ValueError("Value must be a non-empty string.")
        return value.strip()

    @field_validator("metadata_", mode="before")
    @classmethod
    def _cast_metadata(cls, v: MetaData | dict) -> MetaData:
        if isinstance(v, MetaData):
            return v
        if isinstance(v, dict):
            # let MetaData do its own validation on timestamps, ids, etc.
            return MetaData(**v)
        raise TypeError(f"metadata must be MetaData or dict, got {type(v).__name__}")

    @field_validator("layout", mode="before")
    @classmethod
    def _cast_layout(cls, v: Layout | dict) -> Layout:
        if isinstance(v, Layout):
            return v
        if isinstance(v, dict):
            # let Layout do its own validation on coordinates, etc.
            return Layout(**v)
        raise TypeError(f"layout must be Layout or dict, got {type(v).__name__}")

    @property
    def id(self) -> str:
        """
        Get the unique identifier of the component
        :return: Unique identifier as a string
        """
        return self._id

    @property
    def strategy(self) -> ExecutionStrategy:
        if self._strategy is None:
            raise ValueError(f"No strategy set for component {self.name}")
        return self._strategy

    @strategy.setter
    def strategy(self, value: ExecutionStrategy):
        if not isinstance(value, ExecutionStrategy):
            raise TypeError(
                f"strategy must be an instance of ExecutionStrategy, "
                f"got {type(value).__name__}"
            )
        self._strategy = value

    @property
    def receiver(self) -> Receiver:
        if self._receiver is None:
            raise ValueError(f"No receiver set for component {self.name}")
        return self._receiver

    @property
    def next_components(self) -> List["Component"]:
        """
        Get the next components in the execution chain
        :return: List of next components
        """
        return self._next_components

    @next_components.setter
    def next_components(self, value: List["Component"]):
        if not isinstance(value, list):
            raise TypeError("next_components must be a list of Component instances")
        self._next_components = value

    @property
    def prev_components(self) -> List["Component"]:
        """
        Get the previous components in the execution chain
        :return: List of previous components
        """
        return self._prev_components

    def add_next(self, nxt: "Component"):
        """
        Add a next component to the current component
        :param nxt: The next component to add
        """
        self._next_components.append(nxt)

    def add_prev(self, prev: "Component"):
        """
        Add a previous component to the current component
        :param prev: The previous component to add
        """
        self._prev_components.append(prev)

    def execute(
        self,
        payload: Any,
        metrics: ComponentMetrics,
    ) -> AsyncIterator[Any]:
        """
        Invoke the strategy’s async `execute`, streaming native outputs.
        Returns an AsyncIterator produced by the async generator.
        """
        return self.strategy.execute(self, payload, metrics)

    @abstractmethod
    @abstractmethod
    async def process_row(
        self, *args: Any, **kwargs: Any
    ) -> AsyncIterator[Dict[str, Any]]:
        """Async generator: yield dict rows."""
        raise NotImplementedError

    @abstractmethod
    async def process_bulk(self, *args: Any, **kwargs: Any) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    async def process_bigdata(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError


def get_strategy(strategy_type: str) -> ExecutionStrategy:
    """
    Factory function to get the appropriate execution strategy based on the type
    """
    if strategy_type == StrategyType.ROW:
        return RowExecutionStrategy()
    elif strategy_type == StrategyType.BULK:
        return BulkExecutionStrategy()
    elif strategy_type == StrategyType.BIGDATA:
        return BigDataExecutionStrategy()
    else:
        raise ValueError(f"Unknown strategy type: {strategy_type}")
