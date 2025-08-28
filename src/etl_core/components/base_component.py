from abc import ABC, abstractmethod
from typing import (
    Optional,
    List,
    Any,
    Dict,
    AsyncIterator,
    ClassVar,
    Sequence,
    Iterable,
)
from uuid import uuid4
import pandas as pd
import dask.dataframe as dd

from pydantic import (
    Field,
    ConfigDict,
    model_validator,
    PrivateAttr,
    field_validator,
)
from enum import Enum

from etl_core.components.dataclasses import MetaData, Layout
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.base_receiver import Receiver
from etl_core.strategies.base_strategy import ExecutionStrategy
from etl_core.strategies.bigdata_strategy import BigDataExecutionStrategy
from etl_core.strategies.bulk_strategy import BulkExecutionStrategy
from etl_core.strategies.row_strategy import RowExecutionStrategy
from etl_core.persistance.base_models.component_base import ComponentBase
from etl_core.components.wiring.schema import Schema
from etl_core.components.envelopes import Out
from etl_core.components.wiring.ports import OutPortSpec, InPortSpec, EdgeRef
from etl_core.components.wiring.validation import (
    validate_row_against_schema,
    validate_dataframe_against_schema,
    validate_dask_dataframe_against_schema,
)


class StrategyType(str, Enum):
    """
    Enum for different strategy types
    """

    ROW = "row"
    BULK = "bulk"
    BIGDATA = "bigdata"


class Component(ComponentBase, ABC):
    """
    Base class for all components in the system.

    Supports both static (class-level) and dynamic (instance-level) ports.
    - Class-level declarations keep simple components declarative.
    - Instance-level extras come from config and are merged during validation.
    """

    # declarations (overridable in subclasses)
    OUTPUT_PORTS: ClassVar[Sequence[OutPortSpec]] = ()
    INPUT_PORTS: ClassVar[Sequence[InPortSpec]] = ()
    ALLOW_NO_INPUTS: ClassVar[bool] = False

    _schema_path_separator: ClassVar[str] = "."

    def _validate_payload_against(
        self, schema: "Schema", payload: Any, *, schema_name: str
    ) -> None:
        if payload is None:
            return
        if isinstance(payload, dict):
            validate_row_against_schema(
                payload, schema, schema_name=schema_name, path_separator=self._schema_path_separator
            )
            return
        if isinstance(payload, pd.DataFrame):
            validate_dataframe_against_schema(
                payload, schema, schema_name=schema_name, path_separator=self._schema_path_separator
            )
            return
        if isinstance(payload, dd.DataFrame):
            validate_dask_dataframe_against_schema(
                payload, schema, schema_name=schema_name, path_separator=self._schema_path_separator
            )
            return
        raise TypeError(
            f"{schema_name}: unsupported payload type {type(payload).__name__}"
        )

    def validate_out_payload(self, port: str, payload: Any) -> None:
        schema = self.schema_for_out_port(port)
        if not schema:
            raise ValueError(f"{self.name}: no schema configured for out port {port!r}")
        self._validate_payload_against(
            schema, payload, schema_name=f"{self.name}.out:{port}"
        )

    def validate_in_payload(self, port: str, payload: Any) -> None:
        schema = self.schema_for_in_port(port)
        if not schema:
            raise ValueError(f"{self.name}: no schema configured for in port {port!r}")
        self._validate_payload_against(
            schema, payload, schema_name=f"{self.name}.in:{port}"
        )

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
        validate_assignment=True,
    )
    _id: str = PrivateAttr(default_factory=lambda: str(uuid4()))

    routes: Dict[str, List[EdgeRef | str]] = Field(
        default_factory=dict,
        description="out_port -> [EdgeRef|target_name]. Use EdgeRef to "
        "specify target input port.",
    )
    out_port_schemas: Dict[str, Schema] = Field(
        default_factory=dict,
        description="out_port -> Schema emitted on the port.",
    )
    in_port_schemas: Dict[str, Schema] = Field(
        default_factory=dict,
        description="in_port -> Schema expected on the port.",
    )

    # dynamic ports supplied via config (merged with class-level ports)
    extra_output_ports: List[OutPortSpec] = Field(
        default_factory=list,
        description="Additional output ports declared by config "
        "(merged with class-level OUTPUT_PORTS).",
    )
    extra_input_ports: List[InPortSpec] = Field(
        default_factory=list,
        description="Additional input ports declared by config "
        "(merged with class-level INPUT_PORTS).",
    )
    layout: Layout = Field(default_factory=lambda: Layout())
    metadata_: MetaData = Field(default_factory=lambda: MetaData(), alias="metadata")

    _next_components: List["Component"] = PrivateAttr(default_factory=list)
    _prev_components: List["Component"] = PrivateAttr(default_factory=list)

    # Runtime, set during wiring in Job creation
    _out_routes: Dict[str, List["Component"]] = PrivateAttr(default_factory=dict)
    _out_edges_in_ports: Dict[str, List[str]] = PrivateAttr(default_factory=dict)

    # these need to be created on the concrete component classes
    _strategy: Optional[ExecutionStrategy] = PrivateAttr(default=None)
    _receiver: Optional[Receiver] = PrivateAttr(default=None)

    @field_validator("extra_output_ports", mode="before")
    @classmethod
    def _coerce_extra_output_ports(
        cls, v: Iterable[OutPortSpec | str | dict] | None
    ) -> List[OutPortSpec]:
        """
        Accept List[OutPortSpec] | List[str] | List[dict], cast to List[OutPortSpec].
        """
        if v is None:
            return []
        result: List[OutPortSpec] = []
        for item in v:
            if isinstance(item, OutPortSpec):
                result.append(item)
            elif isinstance(item, str):
                result.append(OutPortSpec(name=item))
            elif isinstance(item, dict):
                result.append(OutPortSpec(**item))
            else:
                raise TypeError(
                    "extra_output_ports items must be OutPortSpec | str | dict"
                )
        return result

    @field_validator("extra_input_ports", mode="before")
    @classmethod
    def _coerce_extra_input_ports(
        cls, v: Iterable[InPortSpec | str | dict] | None
    ) -> List[InPortSpec]:
        """
        Accept List[InPortSpec] | List[str] | List[dict], cast to List[InPortSpec].
        """
        if v is None:
            return []
        result: List[InPortSpec] = []
        for item in v:
            if isinstance(item, InPortSpec):
                result.append(item)
            elif isinstance(item, str):
                result.append(InPortSpec(name=item))
            elif isinstance(item, dict):
                result.append(InPortSpec(**item))
            else:
                raise TypeError(
                    "extra_input_ports items must be InPortSpec | str | dict"
                )
        return result

    def _merged_out_specs(self) -> List[OutPortSpec]:
        """
        Merge class-level OUTPUT_PORTS with extra_output_ports.
        Deduplicate by name with instance extras winning last.
        """
        merged: Dict[str, OutPortSpec] = {p.name: p for p in self._class_out_specs()}
        for p in self.extra_output_ports:
            merged[p.name] = p
        return list(merged.values())

    def _merged_in_specs(self) -> List[InPortSpec]:
        """
        Merge class-level INPUT_PORTS with extra_input_ports.
        Deduplicate by name with instance extras winning last.
        """
        merged: Dict[str, InPortSpec] = {p.name: p for p in self._class_in_specs()}
        for p in self.extra_input_ports:
            merged[p.name] = p
        return list(merged.values())

    @classmethod
    def _class_out_specs(cls) -> List[OutPortSpec]:
        return list(cls.OUTPUT_PORTS)

    @classmethod
    def _class_in_specs(cls) -> List[InPortSpec]:
        return list(cls.INPUT_PORTS)

    # public instance methods used by wiring/runtime
    def expected_ports(self) -> List[OutPortSpec]:
        return self._merged_out_specs()

    def expected_in_ports(self) -> List[InPortSpec]:
        return self._merged_in_specs()

    def expected_in_port_names(self) -> List[str]:
        return [p.name for p in self._merged_in_specs()]

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

    @model_validator(mode="after")
    def _require_declared_input_ports(self) -> "Component":
        """
        Every component declares at least one input port, unless it is an explicit root.
        """
        has_inputs = bool(self.expected_in_ports())
        if not has_inputs and not getattr(self, "ALLOW_NO_INPUTS", False):
            raise ValueError(
                f"{self.name}: must declare at least one input port via INPUT_PORTS "
                "or extra_input_ports, or set ALLOW_NO_INPUTS = True for a "
                "root/source component."
            )
        return self

    @field_validator("routes")
    @classmethod
    def _no_empty_route_keys(
        cls, v: Dict[str, List[EdgeRef | str]]
    ) -> Dict[str, List[EdgeRef | str]]:
        if any(not k for k in v):
            raise ValueError("routes may not contain empty port names")
        return v

    @field_validator("out_port_schemas")
    @classmethod
    def _no_empty_port_schema_names(cls, v: Dict[str, Schema]) -> Dict[str, Schema]:
        if any(not k for k in v):
            raise ValueError("port_schemas may not contain empty port names")
        return v

    @field_validator("in_port_schemas")
    @classmethod
    def _no_empty_in_port_schema_names(cls, v: Dict[str, Schema]) -> Dict[str, Schema]:
        if any(not k for k in v):
            raise ValueError("in_port_schemas may not contain empty port names")
        return v

    @model_validator(mode="after")
    def _validate_declared_ports_known(self) -> "Component":
        """
        If component declares ports, disallow typos in
        routes/port_schemas/in_port_schemas.
        """
        out_declared = {p.name for p in self.expected_ports()}
        in_declared = {p.name for p in self.expected_in_ports()}

        if out_declared:
            unknown_routes = [k for k in self.routes if k not in out_declared]
            if unknown_routes:
                raise ValueError(
                    f"{self.name}: unknown out port(s) in routes: "
                    f"{sorted(unknown_routes)}"
                )
            unknown_out_schemas = [
                k for k in self.out_port_schemas if k not in out_declared
            ]
            if unknown_out_schemas:
                raise ValueError(
                    f"{self.name}: unknown out port(s) in port_schemas: "
                    f"{sorted(unknown_out_schemas)}"
                )
            # Ensure declared ports appear in routes (empty list is fine)
            for pname in out_declared:
                self.routes.setdefault(pname, [])

        if in_declared:
            unknown_in_schemas = [
                k for k in self.in_port_schemas if k not in in_declared
            ]
            if unknown_in_schemas:
                raise ValueError(
                    f"{self.name}: unknown in port(s) in in_port_schemas: "
                    f"{sorted(unknown_in_schemas)}"
                )
        return self

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
            raise TypeError("next_components must be a list")
        self._next_components = value

    @property
    def prev_components(self) -> List["Component"]:
        """
        Get the previous components in the execution chain
        :return: List of previous components
        """
        return self._prev_components

    @prev_components.setter
    def prev_components(self, value: List["Component"]):
        if not isinstance(value, list):
            raise TypeError("prev_components must be a lis")
        self._prev_components = value

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

    # Expose routing resolved at wiring time
    @property
    def out_routes(self) -> Dict[str, List["Component"]]:
        """
        out_port -> list of concrete successor Components
        (filled during wiring)
        """
        return self._out_routes

    @property
    def out_edges_in_ports(self) -> Dict[str, List[str]]:
        """
        out_port -> list of in_port names that this port routes to
        (filled during wiring)
        """
        return self._out_edges_in_ports

    def schema_for_out_port(self, port: str) -> Optional[Schema]:
        return self.out_port_schemas.get(port)

    def schema_for_in_port(self, port: str) -> Optional[Schema]:
        return self.in_port_schemas.get(port)

    def ensure_schemas_for_used_ports(
        self,
        used_in_ports: Dict[str, int],
        used_out_ports: Dict[str, int],
    ) -> None:
        """
        Called after wiring: any port with edges must have a schema.
        Kept small to stay below complexity limits.
        """
        for p, n in used_out_ports.items():
            if n > 0 and p not in self.out_port_schemas:
                raise ValueError(
                    f"Component {self.name}: out port {p!r} routes to"
                    f" {n} target(s) "
                    "but has no schema in port_schemas"
                )
        for p, n in used_in_ports.items():
            if n > 0 and p not in self.in_port_schemas:
                raise ValueError(
                    f"Component {self.name}: in port {p!r} has {n} "
                    f"upstream edge(s) "
                    "but has no schema in in_port_schemas"
                )

    async def execute(
        self,
        payload: Any,
        metrics: ComponentMetrics,
    ) -> AsyncIterator[Out]:
        """
        Invoke the strategy’s async execute and yield items so callers
        can `async for` over this directly.
        """
        async for item in self.strategy.execute(self, payload, metrics):
            yield item

    @abstractmethod
    async def process_row(self, *args: Any, **kwargs: Any) -> AsyncIterator[Out]:
        """Async generator: yield Out envelopes."""
        raise NotImplementedError

    @abstractmethod
    async def process_bulk(self, *args: Any, **kwargs: Any) -> AsyncIterator[Out]:
        """Async generator: yield Out envelopes."""
        raise NotImplementedError

    @abstractmethod
    async def process_bigdata(self, *args: Any, **kwargs: Any) -> AsyncIterator[Out]:
        """Async generator: yield Out envelopes."""
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
