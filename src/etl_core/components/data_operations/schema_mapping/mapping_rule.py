from __future__ import annotations

from typing import Dict, List, Tuple

from pydantic import BaseModel, Field, field_validator

from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef, DataType
from etl_core.utils.common_helpers import get_leaf_field_map


class FieldMapping(BaseModel):
    """
    A single mapping rule:
      - src_port: logical source port (fan-in)
      - src_path: dotted-path or column name for source.
      - dst_port: target port (fan-out).
      - dst_path: dotted-path or column name for destination.
    """

    src_port: str = Field(..., min_length=1)
    src_path: str = Field(..., min_length=1)
    dst_port: str = Field(..., min_length=1)
    dst_path: str = Field(..., min_length=1)

    @field_validator("src_path", "dst_path")
    @classmethod
    def _no_empty_segments(cls, v: str) -> str:
        if any(part.strip() == "" for part in v.split(".")):
            msg = "paths must not contain empty segments (e.g., 'a..b')"
            raise ValueError(msg)
        return v


def _types_compatible(src: FieldDef, dst: FieldDef) -> bool:
    # Treat PATH as STRING to allow path-like text to map to string fields.
    def norm(dt: DataType) -> DataType:
        return DataType.STRING if dt == DataType.PATH else dt

    return norm(src.data_type) == norm(dst.data_type)


def validate_field_mappings(
    rules: List[FieldMapping],
    *,
    in_port_schemas: Dict[str, Schema],
    out_port_schemas: Dict[str, Schema],
    component_name: str,
    path_separator: str = ".",
) -> None:
    """
    Structural and type validation for mapping rules.

    - ensures each (dst_port, dst_path) is written exactly once
    - ensures ports exist and paths exist in their respective schemas
    - ensures compatible data types for src/dst leaves
    """
    if not rules:
        return

    # 1) collision detection on destination
    seen: set[Tuple[str, str]] = set()
    for r in rules:
        key = (r.dst_port, r.dst_path)
        if key in seen:
            raise ValueError(
                f"{component_name}: duplicate mapping to destination {key!r}"
            )
        seen.add(key)

    # 2) validate existence and type compatibility
    in_leaf_cache: Dict[str, Dict[str, FieldDef]] = {}
    out_leaf_cache: Dict[str, Dict[str, FieldDef]] = {}

    for r in rules:
        src_schema = in_port_schemas.get(r.src_port)
        if not isinstance(src_schema, Schema):
            raise ValueError(
                f"{component_name}: no schema for input port {r.src_port!r}"
            )
        dst_schema = out_port_schemas.get(r.dst_port)
        if not isinstance(dst_schema, Schema):
            raise ValueError(
                f"{component_name}: no schema for output port {r.dst_port!r}"
            )

        if r.src_port not in in_leaf_cache:
            in_leaf_cache[r.src_port] = get_leaf_field_map(src_schema, path_separator)
        if r.dst_port not in out_leaf_cache:
            out_leaf_cache[r.dst_port] = get_leaf_field_map(dst_schema, path_separator)

        src_fd = in_leaf_cache[r.src_port].get(r.src_path)
        if src_fd is None:
            raise ValueError(
                f"{component_name}: unknown source path "
                f"{r.src_port!r}:{r.src_path!r}"
            )

        dst_fd = out_leaf_cache[r.dst_port].get(r.dst_path)
        if dst_fd is None:
            raise ValueError(
                f"{component_name}: unknown destination path "
                f"{r.dst_port!r}:{r.dst_path!r}"
            )

        if not _types_compatible(src_fd, dst_fd):
            raise ValueError(
                f"{component_name}: type mismatch {r.src_port}:{r.src_path} "
                f"({src_fd.data_type}) -> {r.dst_port}:{r.dst_path} "
                f"({dst_fd.data_type})"
            )
