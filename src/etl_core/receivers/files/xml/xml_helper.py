from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, Iterable, List, Generator, Union
import xml.etree.ElementTree as ET
import pandas as pd
import dask.dataframe as dd
from dask import delayed
from etl_core.receivers.files.file_helper import resolve_file_path, open_file



def _flatten_element(elem: ET.Element, prefix: str = "") -> Dict[str, Any]:
    """
    Flatten nested XML elements into a dictionary with dot-separated keys.
    """
    out: Dict[str, Any] = {}

    def walk(e: ET.Element, pfx: str = ""):
        groups: Dict[str, List[ET.Element]] = {}
        for ch in list(e):
            groups.setdefault(ch.tag, []).append(ch)

        if len(groups) == 0:
            text = (e.text or "").strip()
            if pfx:
                out[pfx] = text
            return

        for tag, nodes in groups.items():
            if len(nodes) == 1:
                walk(nodes[0], f"{pfx}.{tag}" if pfx else tag)
            else:
                base = (pfx if tag == "item" else (f"{pfx}.{tag}" if pfx else tag))
                for i, node in enumerate(nodes):
                    walk(node, f"{base}.{i}" if base else str(i))

    walk(elem, prefix)
    return out



def _set_nested(parent: ET.Element, dotted: str, value: str):
    """
    Create nested XML elements along a dot path (incl. index paths like tag.0).
    """
    parts = dotted.split(".")
    node = parent
    i = 0
    while i < len(parts):
        part = parts[i]
        if part.isdigit():
            i += 1
            continue
        next_is_index = (i + 1 < len(parts)) and parts[i + 1].isdigit()
        child = None
        if next_is_index:
            child = ET.SubElement(node, part)
            i += 2
        else:
            for ch in node:
                if ch.tag == part:
                    child = ch
                    break
            if child is None:
                child = ET.SubElement(node, part)
            i += 1
        node = child

    node.text = "" if value is None else str(value)


def dict_to_element(rec: Dict[str, Any], record_tag: str) -> ET.Element:
    """
    Convert a flattened dictionary into an XML <record_tag> element.
    """
    e = ET.Element(record_tag)
    for k, v in rec.items():
        _set_nested(e, k, "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v))
    return e



def iter_xml_records(path: Path, root_tag: str, record_tag: str) -> Generator[Dict[str, Any], None, None]:
    """
    Stream records from an XML file: <root><record>...</record>...</root>
    Uses iterparse for memory-efficient streaming.
    """
    path = resolve_file_path(path)
    context = ET.iterparse(str(path), events=("end",))
    for event, elem in context:
        if elem.tag == record_tag:
            yield _flatten_element(elem)
            elem.clear()

def read_xml_bulk(path: Path, root_tag: str, record_tag: str) -> pd.DataFrame:
    """
    Read the entire XML file into a pandas DataFrame.
    """
    rows = list(iter_xml_records(path, root_tag, record_tag))
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def read_xml_bigdata(path: Path, root_tag: str, record_tag: str) -> dd.DataFrame:
    """
    BigData read: expects a directory with multiple XML files (e.g. part-*.xml).
    Reads each file via delayed -> pandas, then combines with dd.from_delayed
    with correct metadata.
    """
    base = resolve_file_path(path)
    if base.is_dir():
        files = sorted([p for p in base.glob("*.xml") if p.is_file()])
    else:
        files = [base]

    if not files:
        return dd.from_pandas(pd.DataFrame(), npartitions=1)

    @delayed
    def _read_one(p: Path) -> pd.DataFrame:
        return read_xml_bulk(p, root_tag=root_tag, record_tag=record_tag)

    delayed_dfs = [_read_one(p) for p in files]

    # Derive schema (meta) from the first file
    first_df = read_xml_bulk(files[0], root_tag=root_tag, record_tag=record_tag)
    if first_df.empty:
        meta = pd.DataFrame()
    else:
        meta = first_df.head(0)

    return dd.from_delayed(delayed_dfs, meta=meta)



def write_xml_row(path: Path, row: Dict[str, Any], root_tag: str, record_tag: str):
    """
    Append a single record. If the file does not exist, create it.
    Note: For large files this is O(n) due to parse+append+write.
    """
    path = resolve_file_path(path)
    if not path.exists() or path.stat().st_size == 0:
        root = ET.Element(root_tag)
        root.append(dict_to_element(row, record_tag))
        ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)
        return

    tree = ET.parse(str(path))
    root = tree.getroot()
    root.append(dict_to_element(row, record_tag))
    tree.write(path, encoding="utf-8", xml_declaration=True)

def write_xml_bulk(path: Path, data: Union[pd.DataFrame, List[Dict[str, Any]]], root_tag: str, record_tag: str):
    """
    Write multiple records at once to an XML file.
    Accepts either a pandas DataFrame or a list of dicts.
    """
    path = resolve_file_path(path)
    root = ET.Element(root_tag)

    if isinstance(data, pd.DataFrame):
        iterable = (rec for _, rec in data.fillna("").astype(str).iterrows())
        for _, rec in data.iterrows():
            root.append(dict_to_element(rec.to_dict(), record_tag))
    else:
        for rec in data:
            root.append(dict_to_element(rec, record_tag))

    ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)

def write_xml_bigdata(path: Path, data: dd.DataFrame, root_tag: str, record_tag: str):
    """
    Write a Dask DataFrame partition-wise into a directory: part-00000.xml, ...
    """
    outdir = resolve_file_path(path)
    outdir.mkdir(parents=True, exist_ok=True)

    nparts = data.npartitions
    for i in range(nparts):
        pdf = data.get_partition(i).compute()
        root = ET.Element(root_tag)
        for _, row in pdf.iterrows():
            root.append(dict_to_element(row.to_dict(), record_tag))
        out = outdir / f"part-{i:05d}.xml"
        ET.ElementTree(root).write(out, encoding="utf-8", xml_declaration=True)





