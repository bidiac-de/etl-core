from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import dask.dataframe as dd

from etl_core.components.wiring.column_definition import DataType, FieldDef
from etl_core.components.wiring.schema import Schema


class SchemaValidationError(ValueError):
    """Raised when produced data violates the declared schema."""


class OnError(str, Enum):
    RAISE = "raise"
    NULL = "null"
    SKIP = "skip"


@dataclass(frozen=True)
class TypeConversionRule:
    """Configuration for a type conversion rule."""
    column_path: str
    target: DataType
    on_error: OnError = OnError.RAISE


_ITEM = "*"

def _parse_path(path: str) -> Tuple[str, ...]:
    """Split a dotted column path into parts."""
    parts = [seg.strip() for seg in path.split(".")]
    return tuple(seg for seg in parts if seg)


def _pd_dtype(target: DataType) -> Optional[str]:
    """Return a pandas nullable dtype for the given logical type."""
    mapping = {
        DataType.STRING: "string",
        DataType.INTEGER: "Int64",
        DataType.FLOAT: "float64",
        DataType.BOOLEAN: "boolean",
    }
    return mapping.get(target)



def _convert_scalar(value: Any, target: DataType) -> Any:
    """Convert a single scalar value to the target type."""
    if value is None:
        return None

    if target == DataType.STRING:
        return None if pd.isna(value) else str(value)

    if target == DataType.INTEGER:
        if pd.isna(value):
            return None
        if isinstance(value, (bool, np.bool_)):
            raise ValueError("cannot cast bool to integer")
        if isinstance(value, (int, np.integer)):
            return int(value)
        if isinstance(value, (float, np.floating)) and float(value).is_integer():
            return int(value)
        return int(value)

    if target == DataType.FLOAT:
        if pd.isna(value):
            return None
        if isinstance(value, (int, np.integer, float, np.floating)) and not isinstance(
                value, (bool, np.bool_)
        ):
            return float(value)
        return float(value)

    if target == DataType.BOOLEAN:
        if pd.isna(value):
            return None
        if isinstance(value, (bool, np.bool_)):
            return bool(value)
        if isinstance(value, (int, np.integer, float, np.floating)):
            return bool(value)
        s = str(value).strip().lower()
        if s in {"true", "t", "1", "yes", "y"}:
            return True
        if s in {"false", "f", "0", "no", "n"}:
            return False

        raise ValueError(f"cannot coerce '{value}' to boolean")

    return value



def _apply_on_error_row(value: Any, target: DataType, policy: OnError) -> Tuple[bool, Any]:
    """
    Convert a value with error policy in row mode, returning (keep, new_value).
    """
    try:
        return True, _convert_scalar(value, target)
    except Exception:
        if policy == OnError.RAISE:
            raise
        if policy == OnError.NULL:
            return True, None

        return False, None


def _walk_and_convert(
        obj: Any,
        parts: Tuple[str, ...],
        target: DataType,
        policy: OnError,
) -> Tuple[bool, Any]:
    """Recursively walk an object by path parts and convert values."""
    if not parts:
        return _apply_on_error_row(obj, target, policy)

    head, *rest = parts

    if obj is None:
        return True, None

    if head == _ITEM:
        if isinstance(obj, list):
            new_items: List[Any] = []
            for item in obj:
                keep, new_item = _walk_and_convert(item, tuple(rest), target, policy)
                if not keep and policy == OnError.SKIP:
                    return False, obj
                new_items.append(new_item)
            return True, new_items
        return True, obj

    if isinstance(obj, dict):
        if head not in obj:
            return True, obj
        keep, new_val = _walk_and_convert(obj[head], tuple(rest), target, policy)
        if not keep and policy == OnError.SKIP:
            return False, obj
        cloned = dict(obj)
        cloned[head] = new_val
        return True, cloned

    return True, obj


def convert_row_nested(
        row: Dict[str, Any],
        rules: Sequence[TypeConversionRule],
) -> Tuple[bool, Dict[str, Any]]:
    """Apply rules with dotted paths to a single nested row."""
    out = dict(row)
    for r in rules:
        parts = _parse_path(r.column_path)
        keep, mutated = _walk_and_convert(out, parts, r.target, r.on_error)
        if not keep and r.on_error == OnError.SKIP:
            return False, out
        if isinstance(mutated, dict):
            out = mutated
    return True, out


def convert_frame_top_level(
        df: pd.DataFrame,
        rules: Sequence[TypeConversionRule],
) -> pd.DataFrame:
    """Convert top-level DataFrame columns according to rules."""

    if df is None or df.empty or not rules:
        return df

    out = df.copy()

    for r in rules:
        parts = _parse_path(r.column_path)
        if len(parts) != 1:
            continue
        col = parts[0]
        if col not in out.columns:
            continue

        dtype = _pd_dtype(r.target)

        if dtype and r.on_error == OnError.RAISE:
            try:
                out[col] = out[col].astype(dtype)
                continue
            except Exception:
                pass

        def elem(v: Any) -> Any:
            try:
                return _convert_scalar(v, r.target)
            except Exception:
                if r.on_error == OnError.NULL:
                    return None
                if r.on_error == OnError.SKIP:
                    return v
                raise

        out[col] = out[col].map(elem)

        if r.target == DataType.INTEGER:
            try:
                out[col] = out[col].astype("Int64")
            except Exception:
                pass

    return out



def convert_dask_top_level(
        ddf: dd.DataFrame,
        rules: Sequence[TypeConversionRule],
) -> dd.DataFrame:
    """Apply rules to a Dask DataFrame using map_partitions."""

    if ddf is None or not rules:
        return ddf

    def _apply(pdf: pd.DataFrame) -> pd.DataFrame:
        return convert_frame_top_level(pdf, rules)

    try:
        meta = _apply(ddf._meta_nonempty)
    except Exception:
        meta = ddf._meta

    return ddf.map_partitions(_apply, meta=meta)



def derive_out_schema(in_schema: Schema, rules: Sequence[TypeConversionRule]) -> Schema:
    """Derive output schema by applying rules to input schema."""

    new_fields: List[FieldDef] = [f.model_copy(deep=True) for f in in_schema.fields]

    def ensure_path(root: FieldDef, parts: Tuple[str, ...]) -> FieldDef:
        if not parts:
            return root
        head, *rest = parts
        if head == _ITEM:
            if root.data_type != DataType.ARRAY:
                root.data_type = DataType.ARRAY
            if root.item is None:
                root.item = FieldDef(name=_ITEM, data_type=DataType.OBJECT, children=[])
            return ensure_path(root.item, tuple(rest))
        if root.data_type != DataType.OBJECT:
            root.data_type = DataType.OBJECT
        if root.children is None:
            root.children = []
        child = next((c for c in root.children if c.name == head), None)
        if child is None:
            child = FieldDef(name=head, data_type=DataType.OBJECT, children=[])
            root.children.append(child)
        return ensure_path(child, tuple(rest))

    for r in rules:
        parts = _parse_path(r.column_path)
        if not parts:
            continue
        root_name = parts[0]
        root = next((f for f in new_fields if f.name == root_name), None)
        if root is None:
            root = FieldDef(name=root_name, data_type=DataType.OBJECT, children=[])
            new_fields.append(root)
        target_node = ensure_path(root, tuple(parts[1:])) if len(parts) > 1 else root

        if target_node.data_type not in (
                DataType.OBJECT,
                DataType.ARRAY,
                DataType.ENUM,
                DataType.PATH,
        ):
            target_node.data_type = r.target
            if r.on_error == OnError.NULL:
                target_node.nullable = True

    return Schema(fields=new_fields)


def _validate_scalar_against_field(value: Any, fd: FieldDef, path: str) -> None\
        :
    """Check a scalar value against a field definition."""
    if value is None:
        if not fd.nullable:
            raise SchemaValidationError(f"{path}: null not allowed")
        return

    dt = fd.data_type

    if dt == DataType.STRING:
        return

    if dt == DataType.INTEGER:
        if isinstance(value, (bool, np.bool_)):
            raise SchemaValidationError(f"{path}: expected integer, got bool")
        if isinstance(value, (int, np.integer)):
            return
        if isinstance(value, (float, np.floating)) and float(value).is_integer():
            return
        raise SchemaValidationError(f"{path}: expected integer, got {type(value).__name__}")

    if dt == DataType.FLOAT:
        if isinstance(value, (int, np.integer, float, np.floating)) and not isinstance(
                value, (bool, np.bool_)
        ):
            return
        raise SchemaValidationError(f"{path}: expected float, got {type(value).__name__}")

    if dt == DataType.BOOLEAN:
        if isinstance(value, (bool, np.bool_)):
            return
        raise SchemaValidationError(f"{path}: expected boolean, got {type(value).__name__}")


def validate_frame_against_schema(
        df: pd.DataFrame,
        schema: Schema,
) -> None:
    """Validate top-level DataFrame columns against schema."""
    wanted = {f.name: f for f in schema.fields}
    for col, fd in wanted.items():
        if col not in df.columns:
            raise SchemaValidationError(f"missing column '{col}' in DataFrame")
        sample = df[col].dropna()
        if sample.empty:
            continue
        _validate_scalar_against_field(sample.iloc[0], fd, col)