from pathlib import Path
import json, gzip, io
from typing import Any, Dict, List

def open_text_auto(path: Path, mode: str = "rt", encoding: str = "utf-8"):
    """Open text files with optional gzip support.

    If the path ends with .gz, open it via gzip and wrap in a TextIOWrapper.
    Otherwise, open as a normal text file.
    """
    p = str(path)
    if p.endswith(".gz"):
        return io.TextIOWrapper(gzip.open(p, mode.replace("t","")), encoding=encoding)
    return open(path, mode, encoding=encoding)

def load_json_records(path: Path) -> List[Dict[str, Any]]:
    """Load JSON as a list of record dictionaries.

    Supports:
      - JSON array: [ {...}, {...} ]
      - Single JSON object: { ... } -> converted to [ { ... } ]
      - JSON Lines (NDJSON): one JSON object per line
    """
    with open_text_auto(path, "rt") as f:
        text = f.read().strip()
    if not text:
        return []
    if text.startswith("["):
        data = json.loads(text)
        return data if isinstance(data, list) else [data]
    lines = [ln for ln in text.splitlines() if ln.strip()]
    try:
        return [json.loads(ln) for ln in lines]
    except json.JSONDecodeError:
        obj = json.loads(text)
        return obj if isinstance(obj, list) else [obj]

def dump_json_records(path: Path, records: List[Dict[str, Any]], indent: int = 2):
    """Write a list of record dictionaries as a JSON array (UTF-8, ensure_ascii=False)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open_text_auto(path, "wt") as f:
        json.dump(records, f, indent=indent, ensure_ascii=False)
