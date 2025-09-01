from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, Iterable, List, Generator
import xml.etree.ElementTree as ET
import pandas as pd
from etl_core.receivers.files.file_helper import resolve_file_path, open_file
import re
import math

def _is_nullish(v: Any) -> bool:
    if v is None:
        return True
    try:
        return pd.isna(v)
    except Exception:
        return isinstance(v, float) and math.isnan(v)

def element_to_nested(element: ET.Element) -> Any:
    """Convert an XML element into a *truly nested* Python structure.
    Rules:
    - Attributes -> under "@attrs"
    - Mixed content: text stored under "#text" if element also has children
    - Repeated child tags become lists
    - Leaf elements become their text (str)
    """
    if not list(element) and not element.attrib:
        return (element.text or "").strip()

    node: Dict[str, Any] = {}
    if element.attrib:
        node["@attrs"] = dict(element.attrib)

    text = (element.text or "").strip()
    if text and list(element):
        node["#text"] = text
    elif text and not list(element):
        return text

    for child in element:
        child_payload = element_to_nested(child)
        tag = child.tag
        if tag in node:
            if not isinstance(node[tag], list):
                node[tag] = [node[tag]]
            node[tag].append(child_payload)
        else:
            node[tag] = child_payload
    return node


def nested_to_element(tag: str, data: Any) -> ET.Element:
    """Convert a nested dict/list/primitive back to an Element.
    Special keys: "@attrs" for attributes, "#text" for mixed content text.
    Lists create repeated child elements of the same tag.
    """
    el = ET.Element(tag)
    if isinstance(data, dict):
        attrs = data.get("@attrs")
        if isinstance(attrs, dict):
            for k, v in attrs.items():
                el.set(str(k), str(v))
        if "#text" in data:
            text_val = data["#text"]
            el.text = "" if text_val is None else str(text_val)
        for k, v in data.items():
            if k in ("@attrs", "#text"):
                continue
            if isinstance(v, list):
                for item in v:
                    el.append(nested_to_element(k, item))
            else:
                el.append(nested_to_element(k, v))
    elif isinstance(data, list):
        for item in data:
            el.append(nested_to_element(tag, item))
    else:
        el.text = "" if data is None else str(data)
    return el



def _iter_records(path: Path, record_tag: str) -> Generator[Dict[str, Any], None, None]:
    """Stream <record_tag> elements as nested dicts using iterparse (low memory)."""
    path = resolve_file_path(path)
    context = ET.iterparse(str(path), events=("end",))
    for event, elem in context:
        if elem.tag == record_tag:
            yield element_to_nested(elem)
            elem.clear()


def read_xml_row(path: Path, record_tag: str) -> Generator[Dict[str, Any], None, None]:
    return _iter_records(path, record_tag)


def _flatten_to_map(prefix: str, value: Any, out: Dict[str, Any]) -> None:
    """
    Flattens nested structures:
    - dict: keys are concatenated with '.' (e.g. address.street)
    - list: indexed as [i] (e.g. tags[0], tags[1])
    - special keys: '@attrs' -> attributes (e.g. address.@attrs.cityId),
                    '#text'  -> mixed text content (e.g. para.#text)
    """
    if isinstance(value, dict):
        for k, v in value.items():
            if k in ("@attrs", "#text"):
                key = f"{prefix}.{k}" if prefix else k
                if k == "@attrs" and isinstance(v, dict):
                    for ak, av in v.items():
                        akey = f"{key}.{ak}"
                        out[akey] = av
                else:
                    out[key] = v
            else:
                key = f"{prefix}.{k}" if prefix else k
                _flatten_to_map(key, v, out)
    elif isinstance(value, list):
        for i, item in enumerate(value):
            key = f"{prefix}[{i}]"
            _flatten_to_map(key, item, out)
    else:
        out[prefix] = value

def _flatten_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    flat: Dict[str, Any] = {}
    _flatten_to_map("", rec, flat)
    return {k.lstrip("."): v for k, v in flat.items()}

def read_xml_bulk_chunks(
        path: Path,
        record_tag: str,
        *,
        chunk_size: int = 10_000,
) -> Generator[pd.DataFrame, None, None]:
    """
    Yield *flat* pandas DataFrame chunks directly from a file.
    Column names represent the nesting (e.g. 'a.b', 'tags[0]', ...)
    """
    buf_rows: List[Dict[str, Any]] = []
    for rec in _iter_records(path, record_tag):
        buf_rows.append(_flatten_record(rec))
        if len(buf_rows) >= chunk_size:
            yield pd.DataFrame.from_records(buf_rows)
            buf_rows = []
    if buf_rows:
        yield pd.DataFrame.from_records(buf_rows)

def read_xml_bulk_once(path: Path, record_tag: str) -> pd.DataFrame:
    parts = list(read_xml_bulk_chunks(path, record_tag, chunk_size=10_000))
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

def _build_payload_from_row_or_record(obj: Dict[str, Any]) -> Dict[str, Any] | Any:
    """
    Takes either `{"record": {...}}` or a flat dict row.

    * Filters out nulls (None, NaN, pd.NA)
    * Unflattens if keys contain `.` or `[i]`
    * Otherwise keeps the flat mapping as-is
    """

    payload = obj.get("record", obj)
    if not isinstance(payload, dict):
        return {"#text": "" if payload is None else str(payload)}

    cleaned = {k: v for k, v in payload.items() if not _is_nullish(v)}
    if any(("." in k) or ("[" in k and "]" in k) for k in cleaned.keys()):
        return unflatten_record(cleaned)
    return cleaned


def _row_to_element(record_tag: str, row: Dict[str, Any]) -> ET.Element:
    payload = _build_payload_from_row_or_record(row)
    return nested_to_element(record_tag, payload)


def write_xml_bulk(path: Path, data: pd.DataFrame, *, root_tag: str, record_tag: str) -> None:
    path = resolve_file_path(path)
    root = ET.Element(root_tag)

    if not data.empty:
        if "record" in data.columns:
            for rec in data["record"].tolist():
                root.append(nested_to_element(record_tag, rec))
        else:
            for _, r in data.iterrows():
                root.append(_row_to_element(record_tag, r.to_dict()))

    tree = ET.ElementTree(root)
    with open_file(path, "wb") as f:
        tree.write(f, encoding="utf-8", xml_declaration=True)


def write_xml_row(path: Path, row: Dict[str, Any], *, root_tag: str, record_tag: str) -> None:
    path = resolve_file_path(path)
    new_record_el = _row_to_element(record_tag, row)
    new_record_xml = ET.tostring(new_record_el, encoding="unicode")

    if not path.exists() or path.stat().st_size == 0:
        root = ET.Element(root_tag)
        root.append(new_record_el)
        with open_file(path, "wb") as f:
            ET.ElementTree(root).write(f, encoding="utf-8", xml_declaration=True)
        return

    with open_file(path, "r") as f:
        content = f.read()

    closing = f"</{root_tag}>"
    idx = content.rfind(closing)
    if idx == -1:
        # malformed -> recreate safely
        root = ET.Element(root_tag)
        root.append(new_record_el)
        with open_file(path, "wb") as f:
            ET.ElementTree(root).write(f, encoding="utf-8", xml_declaration=True)
        return

    new_content = content[:idx] + new_record_xml + content[idx:]
    with open_file(path, "w") as f:
        f.write(new_content)


_PATH_RE = re.compile(r'\.?([^\.\[\]]+)(?:\[(\d+)\])?')

def _parse_path(path: str):
    return [(m.group(1), m.group(2)) for m in _PATH_RE.finditer(path)]

def _ensure_list(obj, key):
    if key not in obj or not isinstance(obj[key], list):
        obj[key] = []
    return obj[key]

def _ensure_dict(obj, key):
    if key not in obj or not isinstance(obj[key], dict):
        obj[key] = {}
    return obj[key]

def _set_path(root: dict, path: str, value):
    parts = _parse_path(path)
    cur = root
    for i, (name, idx) in enumerate(parts):
        last = (i == len(parts) - 1)

        if name == "@attrs":
            if last:
                return
            attrs = _ensure_dict(cur, "@attrs")
            next_name, _ = parts[i + 1]
            if i + 1 == len(parts) - 1:
                attrs[next_name] = value
                return
            cur = _ensure_dict(attrs, next_name)
            continue

        if name == "#text" and last:
            cur["#text"] = "" if value is None else str(value)
            return

        if idx is None:
            if last:
                cur[name] = value
                return
            cur = _ensure_dict(cur, name)
        else:
            lst = _ensure_list(cur, name)
            j = int(idx)
            while len(lst) <= j:
                lst.append({})
            if last:
                lst[j] = value
                return
            if not isinstance(lst[j], dict):
                lst[j] = {}
            cur = lst[j]

def unflatten_record(flat: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in flat.items():
        if k:
            _set_path(out, k, v)
    return out



