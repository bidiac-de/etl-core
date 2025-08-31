from __future__ import annotations

from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Set,
    TYPE_CHECKING,
)

import pandas as pd

from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef, DataType

if TYPE_CHECKING:
    from etl_core.components.base_component import Component

# Generic / job & testing utils


def get_component_by_name(job: Any, name: str) -> Component:
    """
    Look up a Component in job.components by its unique .name.

    Kept generic on purpose (type: Any) to avoid import loops with RuntimeJob.
    """
    for comp in getattr(job, "components", []):
        if getattr(comp, "name", None) == name:
            return comp  # type: ignore[return-value]
    raise ValueError(f"No component named {name!r} found in job.components")


def normalize_df(
    df: pd.DataFrame, sort_cols: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Stable sort + reset index for robust equality checks across engines.
    Ensures comparisons are not sensitive to ordering.
    """
    cols = (
        list(df.columns)
        if sort_cols is None
        else [c for c in sort_cols if c in df.columns]
    )
    if cols:
        df = df.sort_values(by=cols, kind="mergesort")
    return df.reset_index(drop=True)


def assert_unique(
    items: Iterable[Any],
    *,
    key: Optional[callable] = None,
    context: str = "items",
) -> None:
    """
    Assert that all items in the iterable are unique.
    Optionally accepts a `key` function to derive the comparison value.
    """
    seen = set()
    for idx, item in enumerate(items):
        value = key(item) if key else item
        if value in seen:
            raise ValueError(f"Duplicate {context} found: {value!r} (at index {idx})")
        seen.add(value)


# Schema walking and validation


def required_names(children: Sequence[FieldDef]) -> Set[str]:
    """Return names of non-nullable children."""
    return {c.name for c in children if not c.nullable}


def child_map(children: Sequence[FieldDef]) -> Dict[str, FieldDef]:
    """Map child name -> FieldDef for quick lookup."""
    return {c.name: c for c in children}


def leaf_field_paths_with_defs(
    fields: Sequence[FieldDef],
    sep: str,
    prefix: Optional[str] = None,
) -> List[Tuple[str, FieldDef]]:
    """
    Produce (flattened_path, fielddef) for scalar/enum leaves.
    Arrays-of-scalars (or arrays with item) count as a single 'leaf' column
    for DataFrame validation (row-mode can drill into elements).
    """
    out: List[Tuple[str, FieldDef]] = []
    for f in fields:
        p = f"{prefix}{sep}{f.name}" if prefix else f.name
        if f.data_type == DataType.OBJECT and f.children:
            out.extend(leaf_field_paths_with_defs(f.children, sep, p))
        elif f.data_type == DataType.ARRAY and f.item:
            out.append((p, f))
        else:
            out.append((p, f))
    return out


def leaf_field_paths(schema: Schema, sep: str) -> List[str]:
    """Flattened leaf paths for a whole Schema (DataFrame view)."""
    return [p for p, _ in leaf_field_paths_with_defs(schema.fields, sep)]


def is_null(v: Any) -> bool:
    return v is None


def type_ok_scalar(v: Any, fd: FieldDef) -> bool:
    """
    Python-level type guard for scalar/enum fields.
    Bool is a subclass of int in Python — explicitly exclude for numeric types.
    """
    if is_null(v):
        return fd.nullable

    t = fd.data_type
    if t in (DataType.STRING, DataType.PATH):
        return isinstance(v, str)
    if t == DataType.INTEGER:
        return isinstance(v, int) and not isinstance(v, bool)
    if t == DataType.FLOAT:
        return isinstance(v, (float, int)) and not isinstance(v, bool)
    if t == DataType.BOOLEAN:
        return isinstance(v, bool)
    if t == DataType.ENUM:
        # enums accept primitive scalars; domain check happens elsewhere
        return isinstance(v, (str, int, float, bool)) or v is None
    return True


def enum_ok(v: Any, fd: FieldDef) -> bool:
    """Check enum domain membership (stringified compare), allowing nulls."""
    if fd.data_type != DataType.ENUM or is_null(v) or not fd.enum_values:
        return True
    return str(v) in set(fd.enum_values or [])


def ensure_df_columns(
    df_cols: Iterable[str],
    schema: Schema,
    *,
    schema_name: str,
    sep: str,
) -> None:
    """
    DataFrames are flat: validated against flattened leaf paths.
    Arrays are treated as single columns at path 'a.b' that typically hold lists.
    """
    leafs = leaf_field_paths(schema, sep)
    missing = sorted([c for c in leafs if c not in df_cols])
    if missing:
        raise ValueError(f"{schema_name}: missing required columns {missing}")

    extras = sorted([c for c in df_cols if c not in leafs])
    if extras:
        raise ValueError(f"{schema_name}: unknown columns present {extras}")



def pandas_flatten_docs(docs: List[Dict[str, Any]], sep: str = ".") -> pd.DataFrame:
    """
    Normalize a list of nested documents into a flat DataFrame with dot columns.
    """
    if not docs:
        return pd.DataFrame()
    return pd.json_normalize(docs, sep=sep)


def unflatten_record(flat: Dict[str, Any], sep: str = ".") -> Dict[str, Any]:
    """
    Convert a flat dict with dotted keys into a nested dict.

    Example:
        {'a.b': 1, 'a.c.d': 2} -> {'a': {'b': 1, 'c': {'d': 2}}}
    """
    nested: Dict[str, Any] = {}
    for key, value in flat.items():
        if not key or sep not in key:
            nested[key] = value
            continue

        parts = [p for p in key.split(sep) if p]
        if not parts:
            continue

        cursor: Dict[str, Any] = nested
        for part in parts[:-1]:
            nxt = cursor.get(part)
            if not isinstance(nxt, dict):
                nxt = {}
                cursor[part] = nxt
            cursor = nxt
        cursor[parts[-1]] = value
    return nested


def unflatten_many(records: Iterable[Dict[str, Any]], sep: str = ".") -> List[Dict[str, Any]]:
    """Batch version of unflatten_record."""
    return [unflatten_record(r, sep=sep) for r in records]
