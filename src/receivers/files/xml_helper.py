from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, Iterable, List
import xml.etree.ElementTree as ET

import pandas as pd
from dask import dataframe as dd


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

def read_xml_bulk_records(filepath: Path, record_tag: str) -> List[Dict[str, Any]]:
    tree = ET.parse(filepath)
    root = tree.getroot()
    return element_to_dicts(root, record_tag)

def write_xml_full(filepath: Path, records: List[Dict[str, Any]], root_tag: str, record_tag: str) -> None:
    _ensure_parent(filepath)
    root = dicts_to_element(records, root_tag, record_tag)
    tree = ET.ElementTree(root)
    tree.write(filepath, encoding="utf-8", xml_declaration=True)

def append_xml_row(filepath: Path, row: Dict[str, Any], root_tag: str, record_tag: str) -> None:
    if filepath.exists():
        try:
            tree = ET.parse(filepath)
            root = tree.getroot()
        except ET.ParseError:
            root = ET.Element(root_tag)
            tree = ET.ElementTree(root)
    else:
        root = ET.Element(root_tag)
        tree = ET.ElementTree(root)

    r = ET.SubElement(root, record_tag)
    for k, v in row.items():
        child = ET.SubElement(r, str(k))
        child.text = "" if v is None else str(v)

    _ensure_parent(filepath)
    tree.write(filepath, encoding="utf-8", xml_declaration=True)

def dataframe_to_xml_root(pdf: pd.DataFrame, *, record_tag: str, root_tag: str = "rows") -> ET.Element:
    """Build an XML ElementTree root from a pandas DataFrame."""
    root = ET.Element(root_tag)
    for _, row in pdf.iterrows():
        rec = ET.SubElement(root, record_tag)
        for k, v in row.to_dict().items():
            child = ET.SubElement(rec, str(k))
            if v is None or (isinstance(v, float) and pd.isna(v)):
                child.text = ""
            else:
                child.text = str(v)
    return root

def write_partition_xml(dirpath: Path, pdf: pd.DataFrame, part_idx: int, *, record_tag: str, root_tag: str = "rows") -> None:
    """Write a single partition DataFrame to XML (part-*.xml) in dirpath."""
    root = dataframe_to_xml_root(pdf, record_tag=record_tag, root_tag=root_tag)
    out = Path(dirpath) / f"part-{part_idx:05d}.xml"
    ET.ElementTree(root).write(out, encoding="utf-8", xml_declaration=True)

def ddf_write_partitioned_xml(dirpath: Path, ddf: dd.DataFrame, *, record_tag: str, root_tag: str = "rows") -> None:
    """Write all Dask partitions to XML files under dirpath."""
    Path(dirpath).mkdir(parents=True, exist_ok=True)
    for i in range(ddf.npartitions):
        pdf = ddf.get_partition(i).compute()
        write_partition_xml(dirpath, pdf, i, record_tag=record_tag, root_tag=root_tag)

def dask_row_count(ddf: dd.DataFrame) -> int:
    """Robust row counting for Dask DataFrames (mirrors JSON logic)."""
    try:
        return int(ddf.shape[0].compute())
    except Exception:
        try:
            return int(ddf.index.size.compute())
        except Exception:
            return int(
                ddf.map_partitions(lambda df: pd.Series([len(df)])).compute().sum()
            )