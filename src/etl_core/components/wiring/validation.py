from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd
import dask.dataframe as dd

from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef, DataType
from etl_core.utils.common_helpers import (
    required_names,
    child_map,
    leaf_field_paths_with_defs,
    type_ok_scalar,
    enum_ok,
    ensure_df_columns,
)


@dataclass(frozen=True)
class _Ctx:
    schema_name: str
    sep: str


# tiny helpers


def _require_map(obj: Any, ctx: _Ctx, path: str) -> Dict[str, Any]:
    if not isinstance(obj, dict):
        raise ValueError(f"{ctx.schema_name}: '{path}' must be an object (dict)")
    return obj


def _require_list(obj: Any, ctx: _Ctx, path: str) -> List[Any]:
    if not isinstance(obj, list):
        raise ValueError(f"{ctx.schema_name}: '{path}' must be an array (list)")
    return obj


# row validation helpers


def _validate_object_row(value: Any, fd: FieldDef, ctx: _Ctx, path: str) -> None:
    """
    Validate values for an OBJECT field.
    Behavior notes:
    - None -> allowed only if nullable
    - required/unknown keys checked against children
    - recurse into children with path dotted by ctx.sep
    """
    obj = None if value is None else _require_map(value, ctx, path)
    if obj is None:
        if not fd.nullable:
            raise ValueError(f"{ctx.schema_name}: '{path}' is required (object)")
        return

    req = required_names(fd.children or [])
    fm = child_map(fd.children or [])
    missing = sorted([n for n in req if n not in obj])
    if missing:
        raise ValueError(f"{ctx.schema_name}: '{path}' missing fields {missing}")

    unknown = sorted([k for k in obj if k not in fm])
    if unknown:
        raise ValueError(f"{ctx.schema_name}: '{path}' has unknown fields {unknown}")

    for k, sub in fm.items():
        _validate_value_row(obj.get(k), sub, ctx, f"{path}{ctx.sep}{k}")


def _validate_array_row(value: Any, fd: FieldDef, ctx: _Ctx, path: str) -> None:
    """
    Validate values for an ARRAY field.
    Behavior notes:
    - None -> allowed only if nullable
    - if fd.item is None: structural validation only (type=list), no element checks
    - otherwise: recurse element-wise with '[idx]' suffix in the path
    """
    arr = None if value is None else _require_list(value, ctx, path)
    if arr is None:
        if not fd.nullable:
            raise ValueError(f"{ctx.schema_name}: '{path}' is required (array)")
        return

    if fd.item is None:
        # structure-only arrays: we've already asserted it's a list
        return

    for idx, el in enumerate(arr):
        _validate_value_row(el, fd.item, ctx, f"{path}[{idx}]")


def _validate_scalar_row(value: Any, fd: FieldDef, ctx: _Ctx, path: str) -> None:
    """
    Validate scalar/enum leaves.
    We first check python-level type compatibility, then enum domain membership.
    """
    if not type_ok_scalar(value, fd):
        tname = type(value).__name__
        raise ValueError(
            f"{ctx.schema_name}: '{path}' expected {fd.data_type}, got {tname}"
        )
    if not enum_ok(value, fd):
        raise ValueError(
            f"{ctx.schema_name}: '{path}' must be one of {fd.enum_values}, "
            f"got {value!r}"
        )


def _validate_value_row(value: Any, fd: FieldDef, ctx: _Ctx, path: str) -> None:
    """
    Validate any value against a FieldDef. Split into small, purpose-built
    helpers to keep the logic readable and easy to audit.
    """
    dt = fd.data_type
    if dt == DataType.OBJECT:
        _validate_object_row(value, fd, ctx, path)
        return

    if dt == DataType.ARRAY:
        _validate_array_row(value, fd, ctx, path)
        return

    # Scalars and enums come here
    _validate_scalar_row(value, fd, ctx, path)


# public validator entry points


def validate_row_against_schema(
    payload: Dict[str, Any],
    schema: Schema,
    *,
    schema_name: str = "row",
    path_separator: str = ".",
) -> None:
    """
    Validate a nested dict against a nested Schema.
    - checks required/unknown fields
    - checks types recursively (object/array/scalars)
    - checks enum domains
    """
    ctx = _Ctx(schema_name=schema_name, sep=path_separator)
    root = _require_map(payload, ctx, "$")

    req = required_names(schema.fields)
    fmap = child_map(schema.fields)
    missing = sorted([n for n in req if n not in root])
    if missing:
        raise ValueError(f"{schema_name}: missing required fields {missing}")
    unknown = sorted([k for k in root if k not in fmap])
    if unknown:
        raise ValueError(f"{schema_name}: unknown fields present {unknown}")

    for k, sub in fmap.items():
        _validate_value_row(root.get(k), sub, ctx, k)


def _enum_and_null_checks_pandas(
    df: pd.DataFrame,
    schema: Schema,
    *,
    schema_name: str,
    sep: str,
) -> None:
    for path, fd in leaf_field_paths_with_defs(schema.fields, sep):
        s = df[path]
        if not fd.nullable and s.isna().any():
            raise ValueError(f"{schema_name}: column '{path}' contains nulls")
        if fd.data_type == DataType.ENUM and fd.enum_values:
            allowed = set(fd.enum_values)
            bad = ~s.fillna("").astype(str).isin(allowed)
            if bad.any():
                raise ValueError(
                    f"{schema_name}: column '{path}' has values "
                    f"outside {sorted(allowed)}"
                )


def validate_dataframe_against_schema(
    df: pd.DataFrame,
    schema: Schema,
    *,
    schema_name: str = "dataframe",
    path_separator: str = ".",
) -> None:
    """
    Validate a pandas DataFrame for a nested schema by comparing to **flattened**
    leaf paths (object children expanded via dots). We enforce:
      * column presence (1:1 with leaves)
      * non-null for non-nullable leaves
      * enum membership
    """
    ensure_df_columns(df.columns, schema, schema_name=schema_name, sep=path_separator)
    _enum_and_null_checks_pandas(
        df, schema, schema_name=schema_name, sep=path_separator
    )


def _enum_and_null_checks_dask(
    ddf: dd.DataFrame,
    schema: Schema,
    *,
    schema_name: str,
    sep: str,
) -> None:
    for path, fd in leaf_field_paths_with_defs(schema.fields, sep):
        s = ddf[path]
        if not fd.nullable and s.isna().any().compute():
            raise ValueError(f"{schema_name}: column '{path}' contains nulls")
        if fd.data_type == DataType.ENUM and fd.enum_values:
            allowed = set(fd.enum_values)
            bad_any = (~s.fillna("").astype(str).isin(allowed)).any().compute()
            if bad_any:
                raise ValueError(
                    f"{schema_name}: column '{path}' has values "
                    f"outside {sorted(allowed)}"
                )


def validate_dask_dataframe_against_schema(
    ddf: dd.DataFrame,
    schema: Schema,
    *,
    schema_name: str = "bigdataframe",
    path_separator: str = ".",
) -> None:
    """
    Same as pandas but computed lazily. Columns are compared to flattened leaves.
    """
    ensure_df_columns(ddf.columns, schema, schema_name=schema_name, sep=path_separator)
    _enum_and_null_checks_dask(ddf, schema, schema_name=schema_name, sep=path_separator)
