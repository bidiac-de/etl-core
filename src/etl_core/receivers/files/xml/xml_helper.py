from __future__ import annotations
import io
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple
import xml.etree.ElementTree as ET

# -----------------------------
# Low-level file helpers
# -----------------------------

def _atomic_overwrite(path: Path, writer):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("wb", delete=False, dir=str(path.parent)) as tmp:
        tmp_path = Path(tmp.name)
    try:
        writer(tmp_path)
        os.replace(tmp_path, path)
    finally:
        try:
            tmp_path.unlink(missing_ok=True)  # type: ignore[attr-defined]
        except Exception:
            pass

# -----------------------------
# Dict <-> XML conversion (nested)
# -----------------------------

TEXT_KEY = "_text"   # if you ever need to preserve mixed content
ATTR_KEY = "_attr"   # for attributes (optional; not used by default)


def dict_to_element(data: Dict[str, Any], tag: str) -> ET.Element:
    """Recursively convert a nested dict to an Element.
    - dict values -> child elements
    - lists -> repeated child elements with the same tag name
    - primitives -> text
    """
    el = ET.Element(tag)
    for k, v in data.items():
        if k in (TEXT_KEY, ATTR_KEY):
            # Reserved keys (not used by default)
            if k == TEXT_KEY and isinstance(v, (str, int, float, bool)):
                el.text = str(v)
            elif k == ATTR_KEY and isinstance(v, dict):
                for ak, av in v.items():
                    el.set(str(ak), str(av))
            continue

        if isinstance(v, list):
            # For lists, repeat the child tag name `k`
            for item in v:
                if isinstance(item, dict):
                    el.append(dict_to_element(item, str(k)))
                else:
                    child = ET.Element(str(k))
                    child.text = "" if item is None else str(item)
                    el.append(child)
        elif isinstance(v, dict):
            el.append(dict_to_element(v, str(k)))
        else:
            child = ET.Element(str(k))
            child.text = "" if v is None else str(v)
            el.append(child)
    return el


def _merge_same_tag_children(children: List[ET.Element]) -> Dict[str, Any]:
    """Group children by tag; if >1 child with same tag -> list.
    Otherwise store as single value/dict.
    """
    groups: Dict[str, List[ET.Element]] = {}
    for c in children:
        groups.setdefault(c.tag, []).append(c)

    out: Dict[str, Any] = {}
    for tag, els in groups.items():
        if len(els) == 1:
            out[tag] = element_to_dict(els[0])
        else:
            out[tag] = [element_to_dict(e) for e in els]
    return out


def element_to_dict(el: ET.Element) -> Any:
    """Recursively convert an Element to nested dict/list/primitive.
    - If element has children -> dict mapping tag -> (value or list of values)
    - If only text -> return text (string). Empty -> "".
    - Attributes are ignored by default (add via ATTR_KEY if needed).
    """
    children = list(el)
    if children:
        d = _merge_same_tag_children(children)
        # include text if there is text other than whitespace
        text = (el.text or "").strip()
        if text:
            d[TEXT_KEY] = text
        # attributes (optional):
        if el.attrib:
            d[ATTR_KEY] = {k: v for k, v in el.attrib.items()}
        return d
    # leaf -> primitive text
    return (el.text or "").strip()


# -----------------------------
# Streaming parse / emit
# -----------------------------

class XMLStreamingError(Exception):
    pass


def iter_xml_records(
        path: Path,
        *,
        record_tag: str,
        on_error: Optional[callable] = None,
) -> Iterator[Dict[str, Any]]:
    """Iterative, low-memory reader yielding one record (dict) per `record_tag`.
    Uses ElementTree.iterparse and clears elements to keep memory low.
    """
    if not path.exists():
        raise FileNotFoundError(str(path))

    try:
        # "end" catches a tag once fully built
        for event, elem in ET.iterparse(path, events=("end",)):
            if elem.tag != record_tag:
                continue
            try:
                yield element_to_record(elem)
            except Exception as exc:
                if on_error:
                    on_error(exc)
            finally:
                elem.clear()  # free memory for long files
    except ET.ParseError as exc:
        raise XMLStreamingError(f"XML parse error: {exc}") from exc


def element_to_record(elem: ET.Element) -> Dict[str, Any]:
    """Convert a <record_tag> element to a dict.
    If element_to_dict returns a primitive, wrap as {"_value": ...}.
    """
    val = element_to_dict(elem)
    if isinstance(val, dict):
        return val
    return {"_value": val}


def records_to_xml_bytes(
        records: List[Dict[str, Any]], *, root_tag: str, record_tag: str
) -> bytes:
    root = ET.Element(root_tag)
    for rec in records:
        root.append(dict_to_element(rec, record_tag))
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def write_records_as_xml(
        path: Path,
        records: List[Dict[str, Any]],
        *,
        root_tag: str,
        record_tag: str,
) -> None:
    def _writer(tmp: Path):
        tree = ET.ElementTree(ET.fromstring(records_to_xml_bytes(records, root_tag=root_tag, record_tag=record_tag)))
        tree.write(tmp, encoding="utf-8", xml_declaration=True)
    _atomic_overwrite(path, _writer)


# -----------------------------
# Schema validation utilities
# -----------------------------
from enum import Enum
from pydantic import BaseModel

class DataType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    OBJECT = "object"
    ARRAY = "array"
    ENUM = "enum"
    PATH = "path"

class FieldDef(BaseModel):
    name: str
    data_type: DataType
    nullable: bool = False
    enum_values: Optional[List[str]] = None
    children: Optional[List["FieldDef"]] = None  # for OBJECT
    item: Optional["FieldDef"] = None            # for ARRAY

FieldDef.model_rebuild()

class Schema(BaseModel):
    fields: List[FieldDef]


def _is_null(v: Any) -> bool:
    return v is None or v == ""  # basic null heuristic for XML text


def _coerce_primitive_to_type(v: Any, t: DataType) -> Any:
    if _is_null(v):
        return None
    if t == DataType.STRING or t == DataType.PATH:
        return str(v)
    if t == DataType.INTEGER:
        return int(v)
    if t == DataType.FLOAT:
        return float(v)
    if t == DataType.BOOLEAN:
        # accept various XML-ish boolean strings
        if isinstance(v, bool):
            return v
        s = str(v).strip().lower()
        if s in ("1", "true", "yes", "y"): return True
        if s in ("0", "false", "no", "n"): return False
        raise ValueError(f"Cannot coerce '{v}' to boolean")
    # ENUM/OBJECT/ARRAY handled elsewhere
    return v


def _validate_field(name: str, spec: FieldDef, value: Any) -> Any:
    """Validate and (for primitives) coerce `value` according to `spec`.
    Return the (possibly coerced) value. Raise on mismatch.
    """
    if _is_null(value):
        if spec.nullable:
            return None
        raise ValueError(f"Field '{name}' is not nullable")

    dt = spec.data_type
    if dt in (DataType.STRING, DataType.INTEGER, DataType.FLOAT, DataType.BOOLEAN, DataType.PATH):
        return _coerce_primitive_to_type(value, dt)

    if dt == DataType.ENUM:
        if not isinstance(value, (str, int, float)):
            raise ValueError(f"Field '{name}' must be a scalar for ENUM")
        sv = str(value)
        if not spec.enum_values or sv not in spec.enum_values:
            raise ValueError(f"Field '{name}' not in enum: {spec.enum_values}")
        return sv

    if dt == DataType.OBJECT:
        if not isinstance(value, dict):
            raise ValueError(f"Field '{name}' must be an object")
        children = spec.children or []
        out: Dict[str, Any] = {}
        for ch in children:
            if ch.name not in value:
                if ch.nullable:
                    out[ch.name] = None
                    continue
                raise ValueError(f"Missing field '{name}.{ch.name}'")
            out[ch.name] = _validate_field(f"{name}.{ch.name}", ch, value[ch.name])
        return out

    if dt == DataType.ARRAY:
        if not isinstance(value, list):
            raise ValueError(f"Field '{name}' must be an array")
        if not spec.item:
            raise ValueError(f"Field '{name}' has ARRAY type but no 'item' spec")
        return [ _validate_field(f"{name}[{i}]", spec.item, v) for i, v in enumerate(value) ]

    raise ValueError(f"Unsupported data type for field '{name}': {dt}")


def validate_record(record: Dict[str, Any], schema: Optional[Schema]) -> Dict[str, Any]:
    if not schema:
        return record
    out: Dict[str, Any] = {}
    for f in schema.fields:
        if f.name not in record:
            if f.nullable:
                out[f.name] = None
                continue
            raise ValueError(f"Missing required field '{f.name}'")
        out[f.name] = _validate_field(f.name, f, record[f.name])
    return out




