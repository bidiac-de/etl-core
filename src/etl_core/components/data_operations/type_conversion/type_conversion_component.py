from __future__ import annotations

from typing import Any, AsyncIterator, Dict, List, Optional

import dask.dataframe as dd
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator

from etl_core.components.base_component import Component
from etl_core.components.component_registry import register_component
from etl_core.components.envelopes import Out
from etl_core.components.wiring.ports import InPortSpec, OutPortSpec
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.data_operations_receivers.type_conversion.type_conversion_helper import (
    OnError,
    TypeConversionRule,
    Schema,
    derive_out_schema,
)
from etl_core.receivers.data_operations_receivers.type_conversion.type_conversion_receiver import (
    TypeConversionReceiver,
)


class TypeConversionRuleModel(BaseModel):
    """Schema model for a type conversion rule."""

    column_path: str = Field(..., description="Dot path; use '*' for list items.")
    target: Any = Field(..., description="Logical target data type.")
    on_error: OnError = Field(default=OnError.RAISE, description="raise|null|skip")

    model_config = ConfigDict(frozen=True)

    def to_runtime(self) -> TypeConversionRule:
        """Convert to runtime rule object."""

        return TypeConversionRule(
            column_path=self.column_path,
            target=self.target,
            on_error=self.on_error,
        )


@register_component("type_conversion")
class TypeConversionComponent(Component):
    """
    Component for type conversion with row, bulk and bigdata processing
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

    INPUT_PORTS = (InPortSpec(name="in", required=True, fanin="many"),)
    OUTPUT_PORTS = (OutPortSpec(name="out", required=True, fanout="many"),)

    rules: List[TypeConversionRuleModel] = Field(
        default_factory=list, description="List of conversion rules."
    )

    _receiver: Optional[TypeConversionReceiver] = PrivateAttr(default=None)
    _runtime_rules: List[TypeConversionRule] = PrivateAttr(default_factory=list)

    @model_validator(mode="after")
    def _build_objects(self) -> "TypeConversionComponent":
        """Initialize receiver and runtime rules."""

        self._receiver = TypeConversionReceiver()
        self._runtime_rules = [r.to_runtime() for r in self.rules]
        return self

    def ensure_schemas_for_used_ports(
            self,
            used_in_ports: Dict[str, int],
            used_out_ports: Dict[str, int],
    ) -> None:
        """Ensure output schema is derived if needed."""

        if used_out_ports.get("out", 0) > 0 and "out" not in self.out_port_schemas:
            in_schema: Optional[Schema] = self.in_port_schemas.get("in")
            if in_schema is not None:
                self.out_port_schemas["out"] = derive_out_schema(
                    in_schema, self._runtime_rules
                )

        return super().ensure_schemas_for_used_ports(used_in_ports, used_out_ports)

    async def process_row(
            self,
            row: Dict[str, Any],
            metrics: ComponentMetrics,
    ) -> AsyncIterator[Out]:
        """Process a single row and yield converted output."""

        if self._receiver is None:
            raise RuntimeError("TypeConversionReceiver not initialized in process_row")
        async for port, payload in self._receiver.process_row(
                row=row, rules=self._runtime_rules, metrics=metrics
        ):
            yield Out(port=port, payload=payload)

    async def process_bulk(
            self,
            dataframe: pd.DataFrame,
            metrics: ComponentMetrics,
    ) -> AsyncIterator[Out]:
        """Process a pandas DataFrame and yield converted output."""

        if self._receiver is None:
            raise RuntimeError("TypeConversionReceiver not initialized in process_bulk")
        out_schema: Optional[Schema] = self.out_port_schemas.get("out")
        async for port, payload in self._receiver.process_bulk(
                dataframe=dataframe,
                rules=self._runtime_rules,
                metrics=metrics,
                out_schema=out_schema,
        ):
            yield Out(port=port, payload=payload)

    async def process_bigdata(
            self,
            ddf: dd.DataFrame,
            metrics: ComponentMetrics,
    ) -> AsyncIterator[Out]:
        """Process a Dask DataFrame and yield converted output."""

        if self._receiver is None:
            raise RuntimeError(
                "TypeConversionReceiver not initialized in process_bigdata"
            )
        async for port, payload in self._receiver.process_bigdata(
                ddf=ddf, rules=self._runtime_rules, metrics=metrics
        ):
            yield Out(port=port, payload=payload)