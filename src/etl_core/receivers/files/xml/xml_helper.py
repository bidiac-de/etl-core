from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List, Generator, Iterator, Tuple
import xml.etree.ElementTree as ET
import pandas as pd
from etl_core.receivers.files.file_helper import resolve_file_path, open_file
import re

TEXT = "#text"
ATTRS = "@attrs"


def element_to_nested(element: ET.Element) -> Any:
    """Convert an XML element into a *truly nested* Python structure.
    Rules:
    - Attributes -> under "@attrs"
    - Mixed content: text stored under "#text" if element also has children
    - Repeated child tags become lists
    - Leaf elements become their text (str)
    """

    def _merge_child(node: Dict[str, Any], tag: str, payload: Any) -> None:
        """Merge a child payload under 'tag', turning it into a list if repeated."""
        if tag in node:
            if not isinstance(node[tag], list):
                node[tag] = [node[tag]]
            node[tag].append(payload)
        else:
            node[tag] = payload

    children = list(element)
    has_children = bool(children)
    has_attrs = bool(element.attrib)
    text = (element.text or "").strip()

    if not has_children and not has_attrs:
        return text

    node: Dict[str, Any] = {}
    if has_attrs:
        node[ATTRS] = dict(element.attrib)

    if text:
        if has_children:
            node[TEXT] = text
        else:
            return text

    for child in children:
        _merge_child(node, child.tag, element_to_nested(child))

    return node


def nested_to_element(tag: str, data: Any) -> ET.Element:
    """
    Convert a nested dict/list/primitive back to an Element.
    Special keys: "@attrs" for attributes, "#text" for mixed content text.
    Lists create repeated child elements of the same tag.
    """
    el = ET.Element(tag)
    _apply_node(el, data)
    return el


def _apply_node(el: ET.Element, data: Any) -> None:
    if isinstance(data, dict):
        _apply_attrs_and_text(el, data)
        for k, v in data.items():
            if k in (ATTRS, TEXT):
                continue
            _append_child(el, k, v)
    elif isinstance(data, list):
        for item in data:
            el.append(nested_to_element(el.tag, item))
    else:
        el.text = "" if data is None else str(data)


def _apply_attrs_and_text(el: ET.Element, data: Dict[str, Any]) -> None:
    attrs = data.get(ATTRS)
    if isinstance(attrs, dict):
        for k, v in attrs.items():
            el.set(str(k), str(v))
    if TEXT in data:
        text_val = data[TEXT]
        el.text = "" if text_val is None else str(text_val)


def _append_child(el: ET.Element, key: str, value: Any) -> None:
    if isinstance(value, list):
        for item in value:
            el.append(nested_to_element(key, item))
    else:
        el.append(nested_to_element(key, value))


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
    """Flatten nested structures into dot / index paths."""
    if isinstance(value, dict):
        _flatten_dict(prefix, value, out)
    elif isinstance(value, list):
        _flatten_list(prefix, value, out)
    else:
        out[prefix] = value


def _flatten_dict(prefix: str, d: Dict[str, Any], out: Dict[str, Any]) -> None:
    attrs = d.get(ATTRS)
    if isinstance(attrs, dict):
        base = _join(prefix, ATTRS)
        for ak, av in attrs.items():
            out[_join(base, ak)] = av

    if TEXT in d:
        out[_join(prefix, TEXT)] = d[TEXT]

    for k, v in d.items():
        if k in (ATTRS, TEXT):
            continue
        _flatten_to_map(_join(prefix, k), v, out)


def _flatten_list(prefix: str, lst: list, out: Dict[str, Any]) -> None:
    for i, item in enumerate(lst):
        _flatten_to_map(f"{prefix}[{i}]", item, out)


def _join(prefix: str, key: str) -> str:
    return f"{prefix}.{key}" if prefix else key


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


def build_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Accepts either:
      - flat dict with dotted / [i] keys  -> unflatten_record(...)
      - nested dict                       -> passthrough
    Nulls (None/NaN/pd.NA) are dropped.
    """
    if not isinstance(payload, dict):
        raise TypeError(
            f"Expected dict payload, got {type(payload).__name__}: {payload}"
        )

    if _has_flat_paths(payload):
        return unflatten_record(payload)
    return payload


def _row_to_element(record_tag: str, row: Dict[str, Any]) -> ET.Element:
    payload = build_payload(row)
    return nested_to_element(record_tag, payload)


def _has_flat_paths(d: Dict[str, Any]) -> bool:
    for k in d.keys():
        if "." in k:
            return True
        if "[" in k and "]" in k:
            return True
    return False


def write_xml_bulk(
    path: Path, data: pd.DataFrame, *, root_tag: str, record_tag: str
) -> None:
    path = resolve_file_path(path)
    root = ET.Element(root_tag)

    if not data.empty:
        for _, r in data.iterrows():
            root.append(_row_to_element(record_tag, r.to_dict()))

    tree = ET.ElementTree(root)
    with open_file(path, "wb") as f:
        tree.write(f, encoding="utf-8", xml_declaration=True)


def _append_record_to_file(
    path: Path, root_tag: str, new_record_el: ET.Element
) -> None:
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
        root = ET.Element(root_tag)
        root.append(new_record_el)
        with open_file(path, "wb") as f:
            ET.ElementTree(root).write(f, encoding="utf-8", xml_declaration=True)
        return

    new_content = content[:idx] + new_record_xml + content[idx:]
    with open_file(path, "w") as f:
        f.write(new_content)


def write_xml_row(
    path: Path, row: Dict[str, Any], *, root_tag: str, record_tag: str
) -> None:
    path = resolve_file_path(path)

    if not isinstance(row, dict):
        raise TypeError(f"Row mode expects a nested dict, got {type(row).__name__}")

    new_record_el = nested_to_element(record_tag, row)
    _append_record_to_file(path, root_tag, new_record_el)


_PATH_RE = re.compile(r"\.?([^\.\[\]]+)(?:\[(\d+)\])?")


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
    last_idx = len(parts) - 1

    for i, (name, idx) in enumerate(parts):
        last = i == last_idx

        if name == ATTRS:
            cur = _step_attrs(cur, parts, i, value)
            return

        if name == TEXT:
            _step_text(cur, value, last)
            return

        if idx is None:
            cur = _step_dict(cur, name, value, last)
        else:
            cur = _step_list(cur, name, idx, value, last)


def _step_attrs(cur: dict, parts, i: int, value):
    if i == len(parts) - 1:
        return cur

    attrs = _ensure_dict(cur, ATTRS)
    next_name, _ = parts[i + 1]

    if i + 1 == len(parts) - 1:
        attrs[next_name] = value
        return cur

    return _ensure_dict(attrs, next_name)


def _step_text(cur: dict, value, last: bool) -> None:
    if last:
        cur[TEXT] = "" if value is None else str(value)


def _step_dict(cur: dict, name: str, value, last: bool) -> dict:
    if last:
        cur[name] = value
        return cur
    return _ensure_dict(cur, name)


def _step_list(cur: dict, name: str, idx: str, value, last: bool) -> dict:
    lst = _ensure_list(cur, name)
    j = int(idx)
    while len(lst) <= j:
        lst.append({})

    if last:
        lst[j] = value
        return cur

    if not isinstance(lst[j], dict):
        lst[j] = {}
    return lst[j]


def unflatten_record(flat: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in flat.items():
        if k:
            _set_path(out, k, v)
    return out


def iter_pdf_chunks_core(
    filepath: Path,
    *,
    record_tag: str,
    chunk_size: int,
) -> Iterator[pd.DataFrame]:
    yield from read_xml_bulk_chunks(
        filepath, record_tag=record_tag, chunk_size=chunk_size
    )


def render_rows_to_xml_fragment(pdf: pd.DataFrame, record_tag: str) -> Tuple[int, str]:
    pieces: List[str] = []
    count = 0
    for _, r in pdf.iterrows():
        payload: Dict[str, Any] = build_payload(r.to_dict())
        el = nested_to_element(record_tag, payload)
        pieces.append(ET.tostring(el, encoding="unicode"))
        count += 1
    return count, "".join(pieces)


def wrap_with_root(fragment: str, root_tag: str) -> str:
    return (
        f'<?xml version="1.0" encoding="utf-8"?>\n<{root_tag}>'
        + fragment
        + f"</{root_tag}>"
    )
