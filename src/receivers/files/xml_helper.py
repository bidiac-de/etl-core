from pathlib import Path
import gzip, io
from typing import Any, Dict, Iterable, List, Optional
import xml.etree.ElementTree as ET

def _open_text_auto(path: Path, mode: str = "rt", encoding: str = "utf-8"):
    """Open text files with optional gzip support.

   If the path ends with .gz, open via gzip and wrap in a TextIOWrapper.
   Otherwise, open as a normal text file.
   """
    p = str(path)
    if p.endswith(".gz"):
        bin_mode = mode.replace("t", "")
        return io.TextIOWrapper(gzip.open(p, bin_mode), encoding=encoding)
    return open(path, mode, encoding=encoding)

def elem_to_dict(elem: ET.Element) -> Dict[str, Any]:
    """Convert an XML element into a flat dict.

    - Copies attributes into the dict.
    - For each child element, stores its text (last one wins for duplicate tags).
    - If the element has no children but has text, store it under the element tag.
    """
    d: Dict[str, Any] = {}
    # attributes first
    for k, v in elem.attrib.items():
        d[k] = v
    # children
    for child in list(elem):
        tag = child.tag
        text = (child.text or "").strip()
        # simple merge, duplicate tags -> last one wins
        d[tag] = text
    # plain text content (if no children)
    if not list(elem) and (elem.text or "").strip():
        d[elem.tag] = (elem.text or "").strip()
    return d

def load_xml_records(path: Path, record_tag: Optional[str] = None) -> List[Dict[str, Any]]:
    """Parse XML file into a list of record dicts.
    If record_tag is None, use the first-level child tag under the root.
    <root>
      <record ...> ... </record>
      <record ...> ... </record>
    </root>
    """
    with _open_text_auto(path, "rt") as f:
        tree = ET.parse(f)
    root = tree.getroot()

    # determine record tag
    if record_tag is None:
        children = list(root)
        if not children:
            return []
        record_tag = children[0].tag

    records: List[Dict[str, Any]] = []
    for elem in root.findall(record_tag):
        records.append(_elem_to_dict(elem))
    return records

def dump_xml_records(path: Path, records: List[Dict[str, Any]],
                     root_tag: str = "records", record_tag: str = "record") -> None:
    """Write list of dicts as simple XML."""
    root = ET.Element(root_tag)
    for rec in records:
        item = ET.SubElement(root, record_tag)
        for k, v in rec.items():
            # primitive fields as child elements
            child = ET.SubElement(item, str(k))
            child.text = "" if v is None else str(v)

    # compact write; pretty-printing is omitted on purpose
    tree = ET.ElementTree(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    # no gzip writing here — compress externally if needed
    tree.write(path, encoding="utf-8", xml_declaration=True)
