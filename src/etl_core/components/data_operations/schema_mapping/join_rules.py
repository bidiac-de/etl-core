from __future__ import annotations

from typing import Dict, List, Literal

from pydantic import BaseModel, Field, field_validator

from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef
from etl_core.utils.common_helpers import get_leaf_field_map

JoinHow = Literal["inner", "left", "right", "outer"]


class JoinStep(BaseModel):
    """
    One join step in a plan.
    Row mode: left_on/right_on are dotted paths in nested dicts.
    Bulk/BigData: left_on/right_on are (flattened) column names.
    """

    left_port: str = Field(..., min_length=1)
    right_port: str = Field(..., min_length=1)
    left_on: str = Field(..., min_length=1)
    right_on: str = Field(..., min_length=1)
    how: JoinHow = "inner"
    output_port: str = Field(..., min_length=1)

    @field_validator("left_on", "right_on")
    @classmethod
    def _no_empty_segments(cls, v: str) -> str:
        parts = v.split(".")
        if any(not p.strip() for p in parts):
            raise ValueError("join key paths must not contain empty segments")
        return v


class JoinPlan(BaseModel):
    steps: List[JoinStep] = Field(default_factory=list)


def validate_join_plan(
    plan: JoinPlan,
    *,
    in_port_schemas: Dict[str, Schema],
    out_port_names: set[str],
    component_name: str,
    path_separator: str = ".",
) -> None:
    """
    Ensure that every join step references known ports and that join keys exist
    in the referenced input schemas.
    """
    if not plan.steps:
        return

    in_ports = set(in_port_schemas.keys())

    def _ensure_known_port(role: str, name: str) -> None:
        if name not in in_ports and name not in out_port_names:
            raise ValueError(f"{component_name}: unknown {role} {name!r}")

    # cache leaf maps per input port to avoid repeated work
    leaf_cache: Dict[str, Dict[str, FieldDef]] = {}

    def _ensure_key_in_schema(port_name: str, key: str) -> None:
        schema = in_port_schemas.get(port_name)
        if schema is None:
            # When joining an intermediate output to another port, skip lookup.
            return
        if port_name not in leaf_cache:
            leaf_cache[port_name] = get_leaf_field_map(schema, path_separator)
        if key not in leaf_cache[port_name]:
            raise ValueError(
                f"{component_name}: join key {key!r} not in schema for "
                f"port {port_name!r}"
            )

    for step in plan.steps:
        _ensure_known_port("left_port", step.left_port)
        _ensure_known_port("right_port", step.right_port)

        if step.output_port not in out_port_names:
            raise ValueError(
                f"{component_name}: unknown output_port {step.output_port!r}"
            )

        _ensure_key_in_schema(step.left_port, step.left_on)
        _ensure_key_in_schema(step.right_port, step.right_on)
