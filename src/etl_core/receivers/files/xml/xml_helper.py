from __future__ import annotations
from pathlib import Path
from typing import Tuple, Any, Dict, Iterable, List, Generator, Union
import xml.etree.ElementTree as ET
import pandas as pd
from etl_core.receivers.files.file_helper import resolve_file_path, open_file



def element_to_nested(element: ET.Element) -> Any:
    """Convert an XML element into a *truly nested* Python structure.
    Rules:
    - Attributes -> under "@attrs"
    - Mixed content: text stored under "#text" if element also has children
    - Repeated child tags become lists
    - Leaf elements become their text (str)
    """
    if not list(element) and not element.attrib:
        # pure leaf
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
            # convert to list if repeated
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


def read_xml_bulk_chunks(path: Path, record_tag: str, *, chunk_size: int = 10000) -> Generator[pd.DataFrame, None, None]:
    """Yield pandas DataFrame *chunks* from a single XML file.
    Each row contains the *nested* dict in column 'record'.
    """
    buf: List[Dict[str, Any]] = []
    for rec in _iter_records(path, record_tag):
        buf.append(rec)
        if len(buf) >= chunk_size:
            yield pd.DataFrame({"record": buf})
            buf = []
    if buf:
        yield pd.DataFrame({"record": buf})


def read_xml_bulk_once(path: Path, record_tag: str) -> pd.DataFrame:
    """Read entire file into a single DataFrame (compat layer)."""
    parts = list(read_xml_bulk_chunks(path, record_tag, chunk_size=10_000))
    if parts:
        return pd.concat(parts, ignore_index=True)
    return pd.DataFrame({"record": []})




def _flatten(prefix: str, value: Any) -> Iterable[Tuple[str, Any]]:
    if isinstance(value, dict):
        if len(value) == 1:
            only_k, only_v = next(iter(value.items()))
            if isinstance(only_v, list):
                for idx, item in enumerate(only_v):
                    new_prefix = f"{prefix}[{idx}]" if prefix else f"{only_k}[{idx}]"
                    if prefix:
                        yield from _flatten(new_prefix if not isinstance(item, (dict, list)) else prefix, item)
                        if isinstance(item, (dict, list)):
                            for sub_k, sub_v in _flatten("", item):
                                yield f"{prefix}[{idx}].{sub_k}" if sub_k else f"{prefix}[{idx}]", sub_v
                    else:
                        yield from _flatten(f"{only_k}[{idx}]", item)
                return
        for k, v in value.items():
            new_prefix = f"{prefix}.{k}" if prefix else str(k)
            yield from _flatten(new_prefix, v)

    elif isinstance(value, list):
        for idx, item in enumerate(value):
            new_prefix = f"{prefix}[{idx}]"
            yield from _flatten(new_prefix, item)

    else:
        yield prefix, value


def flatten_records(df: pd.DataFrame, col: str = "record") -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for rec in df[col].tolist():
        flat: Dict[str, Any] = {}
        for k, v in _flatten("", rec):
            if k:
                flat[k] = v
        rows.append(flat)
    return pd.DataFrame(rows)



def write_xml_bulk(path: Path, data: pd.DataFrame, *, root_tag: str, record_tag: str) -> None:
    path = resolve_file_path(path)

    def _row_to_element(row: Dict[str, Any]) -> ET.Element:
        if "record" in row and isinstance(row["record"], dict):
            return nested_to_element(record_tag, row["record"])
        return nested_to_element(record_tag, {k: v for k, v in row.items()})

    root = ET.Element(root_tag)
    if not data.empty:
        if "record" in data.columns:
            for rec in data["record"].tolist():
                root.append(nested_to_element(record_tag, rec))
        else:
            for _, r in data.iterrows():
                root.append(_row_to_element(r.to_dict()))

    tree = ET.ElementTree(root)
    with open_file(path, "wb") as f:
        tree.write(f, encoding="utf-8", xml_declaration=True)


def write_xml_row(path: Path, row: Dict[str, Any], *, root_tag: str, record_tag: str) -> None:
    path = resolve_file_path(path)
    new_record_el = nested_to_element(record_tag, row.get("record", row))
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

