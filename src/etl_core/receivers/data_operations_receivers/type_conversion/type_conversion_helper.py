from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import dask.dataframe as dd

from src.etl_core.components.wiring.column_definition import DataType, FieldDef
from src.etl_core.components.wiring.schema import Schema




class OnError(str, Enum):
    RAISE = "raise"
    NULL = "null"
    DROP = "drop"
    ROUTE_ERROR = "route_error"


@dataclass(frozen=True)
class TypeConversionRule:
    column_path: str
    target: DataType
    on_error: OnError = OnError.NULL




def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"true", "1", "yes", "y"}:
            return True
        if v in {"false", "0", "no", "n"}:
            return False
    raise ValueError(f"cannot cast {value!r} to boolean")


def cast_scalar(value: Any, target: DataType) -> Any:
    if value is None:
        return None
    if target == DataType.STRING:
        return str(value)
    if target == DataType.INTEGER:
        return int(value)
    if target == DataType.FLOAT:
        return float(value)
    if target == DataType.BOOLEAN:
        return _to_bool(value)
    return value




class SchemaValidationError(ValueError):
    pass


_ITEM = "<item>"


def _parse_path(path: str) -> Tuple[str, ...]:
    return tuple(seg.strip() for seg in path.split(".") if seg.strip())


def _follow(fd: FieldDef, parts: Tuple[str, ...]) -> Optional[FieldDef]:
    if not parts:
        return fd
    head, *rest = parts
    if fd.data_type == DataType.ARRAY and head == _ITEM and fd.item is not None:
        return _follow(fd.item, tuple(rest))
    if fd.data_type == DataType.OBJECT and fd.children:
        for ch in fd.children:
            if ch.name == head:
                return _follow(ch, tuple(rest))
    return None


def validate_rules_against_schema(in_schema: Schema, rules: Iterable[TypeConversionRule]) -> None:
    if not in_schema.fields:
        msg = "input schema must contain fields"
        raise SchemaValidationError(msg)

    for r in rules:
        parts = _parse_path(r.column_path)
        if not parts:
            raise SchemaValidationError("empty column_path in rule")
        head = parts[0]
        start: Optional[FieldDef] = next((f for f in in_schema.fields if f.name == head), None)
        if start is None:
            raise SchemaValidationError(f"rule path root '{head}' not found in input schema")
        target_fd = _follow(start, tuple(parts[1:])) if len(parts) > 1 else start
        if target_fd is None:
            raise SchemaValidationError(f"rule path '{r.column_path}' not found in input schema")
        if target_fd.data_type in (DataType.OBJECT, DataType.ARRAY, DataType.ENUM, DataType.PATH):
            msg = f"cannot cast non-scalar field at '{r.column_path}' (type={target_fd.data_type})"
            raise SchemaValidationError(msg)
        if r.target not in (DataType.STRING, DataType.INTEGER, DataType.FLOAT, DataType.BOOLEAN):
            raise SchemaValidationError(f"unsupported target type at '{r.column_path}': {r.target}")



def _validate_scalar(value: Any, fd: FieldDef, path: str) -> None:
    if value is None:
        if not fd.nullable:
            raise SchemaValidationError(f"{path}: null not allowed")
        return
    dt = fd.data_type
    if dt == DataType.STRING:
        return
    if dt == DataType.INTEGER:
        # accept ints OR floats that are mathematically integral (common with NaN columns)
        if isinstance(value, bool):
            raise SchemaValidationError(f"{path}: expected integer, got bool")
        if isinstance(value, int):
            return
        if isinstance(value, float) and value.is_integer():
            return
        raise SchemaValidationError(f"{path}: expected integer, got {type(value).__name__}")
    if dt == DataType.FLOAT:
        if not (isinstance(value, float) or (isinstance(value, int) and not isinstance(value, bool))):
            raise SchemaValidationError(f"{path}: expected float, got {type(value).__name__}")
        return
    if dt == DataType.BOOLEAN:
        if not isinstance(value, bool):
            raise SchemaValidationError(f"{path}: expected boolean, got {type(value).__name__}")
        return


def _validate_node(value: Any, fd: FieldDef, path: str) -> None:
    if value is None:
        if not fd.nullable:
            raise SchemaValidationError(f"{path}: null not allowed")
        return

    if fd.data_type == DataType.OBJECT:
        if not isinstance(value, dict):
            raise SchemaValidationError(f"{path}: expected object, got {type(value).__name__}")
        for child in fd.children or []:
            key = child.name
            _validate_node(value.get(key), child, f"{path}.{key}" if path else key)
        return

    if fd.data_type == DataType.ARRAY:
        if not isinstance(value, list):
            raise SchemaValidationError(f"{path}: expected array, got {type(value).__name__}")
        if fd.item is None:
            return
        for idx, el in enumerate(value):
            _validate_node(el, fd.item, f"{path}.{_ITEM}[{idx}]")
        return

    _validate_scalar(value, fd, path)


def validate_row_against_schema(row: Dict[str, Any], schema: Schema) -> None:
    for f in schema.fields:
        _validate_node(row.get(f.name), f, f.name)


def validate_frame_against_schema(df: pd.DataFrame, schema: Schema) -> None:
    missing = [f.name for f in schema.fields if f.name not in df.columns]
    if missing:
        raise SchemaValidationError(f"DataFrame missing columns: {missing}")
    for f in schema.fields:
        s = df[f.name]
        if not f.nullable and s.isna().any():
            raise SchemaValidationError(f"{f.name}: contains nulls but nullable=False")
        sample = s.dropna().head(50)
        for idx, v in sample.items():
            _validate_scalar(v, f, f"{f.name}[{idx}]")



_DROP = object()


def _apply_on_path(
        data: Any,
        path: Tuple[str, ...],
        fn: Any,
        on_error: OnError,
        errors: List[Dict[str, Any]],
) -> tuple[Any, bool]:
    if not path:
        try:
            return fn(data), False
        except Exception as exc:  # noqa: BLE001
            if on_error == OnError.RAISE:
                raise
            if on_error == OnError.DROP:
                return _DROP, True
            if on_error == OnError.ROUTE_ERROR:
                errors.append({"error": str(exc), "value": data})
                return data, False
            return None, False

    head, *rest = path
    if head == _ITEM:
        if not isinstance(data, list):
            try:
                new_leaf, dropped = _apply_on_path(data, tuple(rest), fn, on_error, errors)
                return new_leaf, dropped
            except Exception as exc:
                if on_error == OnError.RAISE:
                    raise
                if on_error == OnError.DROP:
                    return _DROP, True
                if on_error == OnError.ROUTE_ERROR:
                    errors.append({"error": str(exc), "value": data})
                    return data, False
                return None, False
        new_list: List[Any] = []
        for el in data:
            new_el, dropped = _apply_on_path(el, tuple(rest), fn, on_error, errors)
            if dropped:
                continue
            new_list.append(new_el)
        return new_list, False

    if not isinstance(data, dict):
        try:
            new_leaf, dropped = _apply_on_path(None, tuple(rest), fn, on_error, errors)
            return data if new_leaf is None else new_leaf, dropped
        except Exception as exc:
            if on_error == OnError.RAISE:
                raise
            if on_error == OnError.DROP:
                return _DROP, True
            if on_error == OnError.ROUTE_ERROR:
                errors.append({"error": str(exc), "value": data})
                return data, False
            return None, False

    cur = dict(data)
    nxt = cur.get(head, None)
    if nxt is None:
        if on_error == OnError.RAISE:
            raise KeyError(f"path segment '{head}' missing")
        if on_error == OnError.DROP:
            return _DROP, True
        if on_error == OnError.ROUTE_ERROR:
            errors.append({"error": "not_found", "path": head})
            return cur, False
        return cur, False

    new_val, dropped = _apply_on_path(nxt, tuple(rest), fn, on_error, errors)
    if dropped:
        cur.pop(head, None)
        return cur, False
    cur[head] = new_val
    return cur, False


def convert_row_nested(
        row: Dict[str, Any],
        rules: Sequence[TypeConversionRule],
) -> tuple[Optional[Dict[str, Any]], List[Dict[str, Any]], bool]:
    out: Any = dict(row)
    errors: List[Dict[str, Any]] = []
    for r in rules:
        parts = _parse_path(r.column_path)
        fn = lambda v, t=r.target: cast_scalar(v, t)  # noqa: E731
        out, dropped = _apply_on_path(out, parts, fn, r.on_error, errors)
        if dropped:
            return None, errors, True
    return out, errors, False


def convert_frame_top_level(df: pd.DataFrame, rules: Sequence[TypeConversionRule]) -> pd.DataFrame:
    out = df.copy()
    drop_mask = pd.Series(False, index=out.index)

    for r in rules:
        if "." in r.column_path or _ITEM in r.column_path:
            continue

        col = r.column_path
        if col not in out.columns:
            if r.on_error == OnError.RAISE:
                raise KeyError(f"column {col!r} not in DataFrame")
            if r.on_error == OnError.DROP:
                drop_mask |= True
            continue

        def _apply(x: Any) -> Any:
            try:
                return cast_scalar(x, r.target)
            except Exception:
                if r.on_error == OnError.RAISE:
                    raise
                if r.on_error == OnError.DROP:
                    return _DROP
                return None

        casted = out[col].map(_apply)

        # Zeilen, die wegen DROP raus müssen, markieren
        mask = casted.apply(lambda v: v is _DROP)
        if mask.any():
            drop_mask |= mask
            # Werte an den DROP-Positionen zu None setzen, damit wir Objekt-Dtype konsistent halten
            casted = casted.mask(mask, other=None)

        # <<< NEU: NaN -> None, 23.0 -> 23, alles als echte Python-Objekte >>>
        py_vals: List[Any] = []
        for v in casted.tolist():
            if pd.isna(v):
                py_vals.append(None)
            elif isinstance(v, float) and v.is_integer():
                py_vals.append(int(v))
            else:
                py_vals.append(v)

        casted = pd.Series(py_vals, index=casted.index, dtype="object")
        # >>> END NEU

        out[col] = casted

    if drop_mask.any():
        out = out.loc[~drop_mask].reset_index(drop=True)

    return out

def convert_dask_top_level(ddf: dd.DataFrame, rules: Sequence[TypeConversionRule]) -> dd.DataFrame:
    def _apply(part: pd.DataFrame) -> pd.DataFrame:
        return convert_frame_top_level(part, rules)

    return ddf.map_partitions(_apply, meta=ddf._meta)




def _ensure_child(obj: FieldDef, name: str) -> FieldDef:
    if obj.children is None:
        obj.children = []
    for ch in obj.children:
        if ch.name == name:
            return ch
    child = FieldDef(name=name, data_type=DataType.OBJECT, children=[])
    obj.children.append(child)
    return child


def _ensure_path(root: FieldDef, parts: Tuple[str, ...]) -> FieldDef:
    if not parts:
        return root
    head, *rest = parts
    if head == _ITEM:
        if root.data_type != DataType.ARRAY:
            root.data_type = DataType.ARRAY
            root.item = FieldDef(name=_ITEM, data_type=DataType.OBJECT, children=[])
        assert root.item is not None
        return _ensure_path(root.item, tuple(rest))
    child = _ensure_child(root, head)
    return _ensure_path(child, tuple(rest))



def apply_rules_to_schema(in_schema: Schema, rules: Sequence[TypeConversionRule]) -> Schema:
    import copy
    new_fields = copy.deepcopy(in_schema.fields)

    for r in rules:
        parts = _parse_path(r.column_path)
        if not parts:
            continue
        root_name = parts[0]
        root_fd = next((f for f in new_fields if f.name == root_name), None)
        if root_fd is None:
            root_fd = FieldDef(name=root_name, data_type=DataType.OBJECT, children=[])
            new_fields.append(root_fd)

        target_node = _ensure_path(root_fd, tuple(parts[1:])) if len(parts) > 1 else root_fd

        if target_node.data_type not in (DataType.OBJECT, DataType.ARRAY, DataType.ENUM, DataType.PATH):
            target_node.data_type = r.target
            if r.on_error == OnError.NULL:
                target_node.nullable = True

    return Schema(fields=new_fields)