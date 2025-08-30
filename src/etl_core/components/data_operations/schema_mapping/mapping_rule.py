from __future__ import annotations

from typing import Dict, Tuple

from pydantic import BaseModel, Field, field_validator

from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef, DataType
from etl_core.components.data_operations.rule_helper import (
    ensure_no_empty_path_segments,
)
from etl_core.utils.common_helpers import get_leaf_field_map


Key = Tuple[str, str]  # (dst_port, dst_path)


class FieldMapping(BaseModel):
    """
    A single mapping rule with explicit destination.
    """

    src_port: str = Field(..., min_length=1)
    src_path: str = Field(..., min_length=1)
    dst_port: str = Field(..., min_length=1)
    dst_path: str = Field(..., min_length=1)

    @field_validator("src_path", "dst_path")
    @classmethod
    def _no_empty_segments(cls, v: str) -> str:
        return ensure_no_empty_path_segments(v)


class FieldMappingSrc(BaseModel):
    """
    A rule that omits destination. Used for nested JSON:

    rules_by_dest = {
        "<dst_port>": {
            "<dst_path>": { "src_port": "...", "src_path": "..." }
        }
    }
    """

    src_port: str = Field(..., min_length=1)
    src_path: str = Field(..., min_length=1)

    @field_validator("src_path")
    @classmethod
    def _no_empty_segments(cls, v: str) -> str:
        return ensure_no_empty_path_segments(v)


def _types_compatible(src: FieldDef, dst: FieldDef) -> bool:
    # Treat PATH as STRING to allow path-like text to map to string fields
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


def _build_mapping_for_dest(
    *,
    dst_port: str,
    dst_path: str,
    src_rule: FieldMappingSrc,
    in_leaf_cache: Dict[str, Dict[str, FieldDef]],
    out_leaf_cache: Dict[str, Dict[str, FieldDef]],
    in_port_schemas: Dict[str, Schema],
    out_port_schemas: Dict[str, Schema],
    component_name: str,
    path_separator: str,
) -> FieldMapping:
    """
    Validate a single destination mapping and return a concrete FieldMapping.
    Raises ValueError with clear messages on any validation issue.
    """
    src_leaves = _ensure_leaf_map(
        in_leaf_cache,
        port=src_rule.src_port,
        schemas=in_port_schemas,
        component_name=component_name,
        path_separator=path_separator,
        role="input",
    )
    dst_leaves = _ensure_leaf_map(
        out_leaf_cache,
        port=dst_port,
        schemas=out_port_schemas,
        component_name=component_name,
        path_separator=path_separator,
        role="output",
    )

    src_fd = src_leaves.get(src_rule.src_path)
    if src_fd is None:
        raise ValueError(
            f"{component_name}: unknown source path "
            f"{src_rule.src_port!r}:{src_rule.src_path!r}"
        )

    dst_fd = dst_leaves.get(dst_path)
    if dst_fd is None:
        raise ValueError(
            f"{component_name}: unknown destination path {dst_port!r}:{dst_path!r}"
        )

    if not _types_compatible(src_fd, dst_fd):
        raise ValueError(
            f"{component_name}: type mismatch "
            f"{src_rule.src_port}:{src_rule.src_path} ({src_fd.data_type}) "
            f"-> {dst_port}:{dst_path} ({dst_fd.data_type})"
        )

    return FieldMapping(
        src_port=src_rule.src_port,
        src_path=src_rule.src_path,
        dst_port=dst_port,
        dst_path=dst_path,
    )


def validate_field_mappings(
    rules_by_dest: Dict[str, Dict[str, FieldMappingSrc]],
    *,
    in_port_schemas: Dict[str, Schema],
    out_port_schemas: Dict[str, Schema],
    component_name: str,
    path_separator: str = ".",
) -> Dict[Key, FieldMapping]:
    """
    Validate nested JSON rules and return canonical dict keyed by
    (dst_port, dst_path). The nested dict shape gives inherent uniqueness.
    """
    if not rules_by_dest:
        return {}

    validated: Dict[Key, FieldMapping] = {}
    in_leaf_cache: Dict[str, Dict[str, FieldDef]] = {}
    out_leaf_cache: Dict[str, Dict[str, FieldDef]] = {}

    for dst_port, fields in rules_by_dest.items():
        # Ensure destination port exists (and cache its leaves once)
        _ensure_leaf_map(
            out_leaf_cache,
            port=dst_port,
            schemas=out_port_schemas,
            component_name=component_name,
            path_separator=path_separator,
            role="output",
        )

        for dst_path, src_rule in fields.items():
            key = (dst_port, dst_path)
            if key in validated:
                # Nested dict already enforces uniqueness, keep this guard
                raise ValueError(f"{component_name}: duplicate mapping for {key!r}")

            fm = _build_mapping_for_dest(
                dst_port=dst_port,
                dst_path=dst_path,
                src_rule=src_rule,
                in_leaf_cache=in_leaf_cache,
                out_leaf_cache=out_leaf_cache,
                in_port_schemas=in_port_schemas,
                out_port_schemas=out_port_schemas,
                component_name=component_name,
                path_separator=path_separator,
            )
            validated[key] = fm

    return validated
