from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, Iterable, List
import xml.etree.ElementTree as ET

def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

def dicts_to_element(records: Iterable[Dict[str, Any]], root_tag: str, record_tag: str) -> ET.Element:
    root = ET.Element(root_tag)
    for rec in records:
        r = ET.SubElement(root, record_tag)
        for k, v in rec.items():
            child = ET.SubElement(r, str(k))
            child.text = "" if v is None else str(v)
    return root

def element_to_dicts(root: ET.Element, record_tag: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in root.findall(record_tag):
        d: Dict[str, Any] = {}
        for child in r:
            d[child.tag] = child.text
        out.append(d)
    return out

def read_xml_bulk(filepath: Path, record_tag: str) -> List[Dict[str, Any]]:
    tree = ET.parse(filepath)
    root = tree.getroot()
    return element_to_dicts(root, record_tag)

def write_xml_bulk(filepath: Path, records: List[Dict[str, Any]], root_tag: str, record_tag: str) -> None:
    _ensure_parent(filepath)
    root = dicts_to_element(records, root_tag, record_tag)
    tree = ET.ElementTree(root)
    tree.write(filepath, encoding="utf-8", xml_declaration=True)

def append_xml_row(filepath: Path, row: Dict[str, Any], root_tag: str, record_tag: str) -> None:
    if filepath.exists():
        tree = ET.parse(filepath)
        root = tree.getroot()
    else:
        root = ET.Element(root_tag)
        tree = ET.ElementTree(root)

    r = ET.SubElement(root, record_tag)
    for k, v in row.items():
        child = ET.SubElement(r, str(k))
        child.text = "" if v is None else str(v)

    _ensure_parent(filepath)
    tree.write(filepath, encoding="utf-8", xml_declaration=True)
