from __future__ import annotations

from typing import Any, AsyncIterator, Dict, List, ClassVar

import dask.dataframe as dd
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from src.etl_core.components.base_component import Component
from src.etl_core.components.component_registry import register_component
from src.etl_core.components.envelopes import Out
from src.etl_core.components.wiring.ports import InPortSpec, OutPortSpec
from src.etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from src.etl_core.components.wiring.column_definition import DataType
from src.etl_core.receivers.data_operations_receivers.type_conversion import type_conversion_helper as H
from src.etl_core.receivers.data_operations_receivers.type_conversion.type_conversion_receiver import (
    TypeConversionReceiver,
)
from src.etl_core.receivers.data_operations_receivers.type_conversion.type_conversion_helper import (
    OnError,
)


class TypeConversionRuleModel(BaseModel):
    column_path: str = Field(..., min_length=1)
    target: DataType
    on_error: OnError = Field(default=OnError.NULL)


@register_component("type_conversion")
class TypeConversionComponent(Component):
    """Type conversion with schema validation."""

    INPUT_PORTS: ClassVar[tuple[InPortSpec, ...]] = (InPortSpec(name="in"),)
    OUTPUT_PORTS: ClassVar[tuple[OutPortSpec, ...]] = (
        OutPortSpec(name="out"),
        OutPortSpec(name="errors"),
    )

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

    rules: List[TypeConversionRuleModel] = Field(default_factory=list)

    def _build_objects(self) -> "TypeConversionComponent":
        self._receiver = TypeConversionReceiver()
        return self

    def ensure_schemas_for_used_ports(
            self,
            used_in_ports: Dict[str, int],
            used_out_ports: Dict[str, int],
    ) -> None:
        """Derive out schema first, then defer to base checks."""
        in_schema = self.in_port_schemas.get("in")
        if in_schema:
            rules = self._rule_objs()
            H.validate_rules_against_schema(in_schema, rules)
            self.out_port_schemas["out"] = H.apply_rules_to_schema(in_schema, rules)
        super().ensure_schemas_for_used_ports(used_in_ports, used_out_ports)

    def _rule_objs(self) -> List[H.TypeConversionRule]:
        return [
            H.TypeConversionRule(
                column_path=r.column_path,
                target=r.target,
                on_error=r.on_error,
            )
            for r in self.rules
        ]

    async def process_row(
            self,
            row: Dict[str, Any],
            metrics: ComponentMetrics,
    ) -> AsyncIterator[Out]:
        in_schema = self.in_port_schemas.get("in")
        if in_schema is not None:
            H.validate_row_against_schema(row, in_schema)

        rules = self._rule_objs()
        async for packed in self.receiver.process_row(  # type: ignore[attr-defined]
                row, rules=rules, metrics=metrics
        ):
            payload = packed.get("__payload__")
            errors = packed.get("__errors__") or []

            out_schema = self.out_port_schemas.get("out")
            if payload is not None and out_schema is not None:
                H.validate_row_against_schema(payload, out_schema)

            if errors:
                yield Out("errors", {"row": row, "errors": errors})
            if payload is not None:
                yield Out("out", payload)

    async def process_bulk(
            self,
            dataframe: pd.DataFrame,
            metrics: ComponentMetrics,
    ) -> AsyncIterator[Out]:
        in_schema = self.in_port_schemas.get("in")
        if in_schema is not None:
            H.validate_frame_against_schema(dataframe, in_schema)

        rules = self._rule_objs()
        async for df_out in self.receiver.process_bulk(  # type: ignore[attr-defined]
                dataframe, rules=rules, metrics=metrics
        ):
            out_schema = self.out_port_schemas.get("out")
            if out_schema is not None:
                H.validate_frame_against_schema(df_out, out_schema)
            yield Out("out", df_out)

    async def process_bigdata(
            self,
            ddf: dd.DataFrame,
            metrics: ComponentMetrics,
    ) -> AsyncIterator[Out]:
        in_schema = self.in_port_schemas.get("in")
        if in_schema is not None:
            meta_cols = set(ddf._meta.columns)
            expected = {f.name for f in in_schema.fields}
            missing = sorted(expected - meta_cols)
            if missing:
                raise H.SchemaValidationError(f"Dask DataFrame missing columns: {missing}")

        rules = self._rule_objs()
        async for ddf_out in self.receiver.process_bigdata(  # type: ignore[attr-defined]
                ddf, rules=rules, metrics=metrics
        ):
            out_schema = self.out_port_schemas.get("out")
            if out_schema is not None:
                meta_cols = set(ddf_out._meta.columns)
                expected = {f.name for f in out_schema.fields}
                missing = sorted(expected - meta_cols)
                if missing:
                    raise H.SchemaValidationError(
                        f"Dask DataFrame output missing columns: {missing}"
                    )
            yield Out("out", ddf_out)