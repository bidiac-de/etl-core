from __future__ import annotations

from typing import Dict, List, Tuple

from pydantic import BaseModel, Field, field_validator

from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef, DataType
from etl_core.components.data_operations.rule_helper import (
    ensure_no_empty_path_segments,
)
from etl_core.utils.common_helpers import get_leaf_field_map


class FieldMapping(BaseModel):
    """
    A single mapping rule:
      - src_port: logical source port (fan-in)
      - src_path: dotted-path or column name for source
      - dst_port: target port (fan-out)
      - dst_path: dotted-path or column name for destination
    """

    src_port: str = Field(..., min_length=1)
    src_path: str = Field(..., min_length=1)
    dst_port: str = Field(..., min_length=1)
    dst_path: str = Field(..., min_length=1)

    @field_validator("src_path", "dst_path")
    @classmethod
    def _no_empty_segments(cls, v: str) -> str:
        return ensure_no_empty_path_segments(v)


def _types_compatible(src: FieldDef, dst: FieldDef) -> bool:
    # Treat PATH as STRING to allow path-like text to map to string fields.
    def norm(dt: DataType) -> DataType:
        return DataType.STRING if dt == DataType.PATH else dt

    return norm(src.data_type) == norm(dst.data_type)


def _ensure_leaf_map(
    cache: Dict[str, Dict[str, FieldDef]],
    *,
    port: str,
    schemas: Dict[str, Schema],
    component_name: str,
    path_separator: str,
    role: str,
) -> Dict[str, FieldDef]:
    # Resolve and cache leaf fields for a port schema once
    schema = schemas.get(port)
    if not isinstance(schema, Schema):
        raise ValueError(f"{component_name}: no schema for {role} port {port!r}")
    if port not in cache:
        cache[port] = get_leaf_field_map(schema, path_separator)
    return cache[port]


def _ensure_no_collisions(rules: List["FieldMapping"], *, component_name: str) -> None:
    # Destination (port, path) must be unique across rules
    seen: set[Tuple[str, str]] = set()
    for r in rules:
        key = (r.dst_port, r.dst_path)
        if key in seen:
            raise ValueError(
                f"{component_name}: duplicate mapping to destination {key!r}"
            )
        seen.add(key)


def validate_field_mappings(
    rules: List["FieldMapping"],
    *,
    in_port_schemas: Dict[str, Schema],
    out_port_schemas: Dict[str, Schema],
    component_name: str,
    path_separator: str = ".",
) -> None:
    """
    Validate mapping rules:
      - no duplicate destinations
      - ports exist and paths exist in their schemas
      - compatible leaf types
    """
    if not rules:
        return

    _ensure_no_collisions(rules, component_name=component_name)

    # Cache leaf maps per port to avoid repeated schema traversal
    in_leaf_cache: Dict[str, Dict[str, FieldDef]] = {}
    out_leaf_cache: Dict[str, Dict[str, FieldDef]] = {}

    for r in rules:
        # Get leaf maps for both sides (ensures port schemas are present)
        src_leaves = _ensure_leaf_map(
            in_leaf_cache,
            port=r.src_port,
            schemas=in_port_schemas,
            component_name=component_name,
            path_separator=path_separator,
            role="input",
        )
        dst_leaves = _ensure_leaf_map(
            out_leaf_cache,
            port=r.dst_port,
            schemas=out_port_schemas,
            component_name=component_name,
            path_separator=path_separator,
            role="output",
        )

        # Look up leaf fields by dotted path
        src_fd = src_leaves.get(r.src_path)
        if src_fd is None:
            raise ValueError(
                f"{component_name}: unknown source path "
                f"{r.src_port!r}:{r.src_path!r}"
            )

        dst_fd = dst_leaves.get(r.dst_path)
        if dst_fd is None:
            raise ValueError(
                f"{component_name}: unknown destination path "
                f"{r.dst_port!r}:{r.dst_path!r}"
            )

        # Enforce compatible leaf types
        if not _types_compatible(src_fd, dst_fd):
            raise ValueError(
                f"{component_name}: type mismatch {r.src_port}:{r.src_path} "
                f"({src_fd.data_type}) -> {r.dst_port}:{r.dst_path} "
                f"({dst_fd.data_type})"
            )
