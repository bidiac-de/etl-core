from __future__ import annotations
import gzip
import io
import json
import pandas as pd
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Iterable, Tuple



def open_text_auto(path: Path, mode: str = "rt", encoding: str = "utf-8"):
    """Open text (optionally gzip-compressed) transparently."""
    p = str(path)
    if p.endswith(".gz"):
        return io.TextIOWrapper(gzip.open(p, mode.replace("t", "")), encoding=encoding)
    return open(path, mode, encoding=encoding)


def is_ndjson_path(path: Path) -> bool:
    """True for .jsonl/.ndjson (optionally .gz)."""
    p = str(path).lower()
    return p.endswith((".jsonl", ".ndjson", ".jsonl.gz", ".ndjson.gz"))


def _coerce_obj_to_record(obj: Any) -> Dict[str, Any]:
    """Wrap non-dict JSON values to keep a stable record shape."""
    return obj if isinstance(obj, dict) else {"_value": obj}


def iter_ndjson_lenient(
        path: Path,
        on_error: Optional[Callable[[Exception], None]] = None,
) -> Iterator[Dict[str, Any]]:
    """Iterate NDJSON records but skip malformed lines."""
    with open_text_auto(path, "rt") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except json.JSONDecodeError as exc:
                if on_error:
                    on_error(exc)
                continue
            yield _coerce_obj_to_record(obj)


def _consume_array_separators(s: str) -> str:
    i = 0
    while i < len(s) and (s[i].isspace() or s[i] == ","):
        i += 1
    return s[i:]


def _read_until_nonspace(f: io.TextIOBase, initial: str, chunk_size: int) -> str:
    buf = initial
    while True:
        stripped = buf.lstrip()
        if stripped:
            return stripped
        chunk = f.read(chunk_size)
        if not chunk:
            return ""
        buf += chunk


def _iter_array_stream(
        f: io.TextIOBase, buf: str, dec: json.JSONDecoder, chunk_size: int
) -> Iterator[Dict[str, Any]]:
    while True:
        buf = _consume_array_separators(buf)
        if buf.startswith("]"):
            return
        if not buf.strip():
            more = f.read(chunk_size)
            if not more:
                return
            buf += more
            continue
        try:
            obj, end = dec.raw_decode(buf)
        except json.JSONDecodeError:
            more = f.read(chunk_size)
            if not more:
                return
            buf += more
            continue
        yield _coerce_obj_to_record(obj)
        buf = buf[end:]


def read_json_row(path: Path, chunk_size: int = 65536) -> Iterator[Dict[str, Any]]:
    """Streaming iterator over JSON content.

    Supported inputs:
      - Single object: { ... }        -> yields the object once
      - JSON array:    [ {...}, ... ] -> yields per element
      - NDJSON (.jsonl/.ndjson)       -> yields per line
      - Gzipped files via open_text_auto()
      - Non-dict values are wrapped as {"_value": <value>}
    """
    if is_ndjson_path(path):
        yield from iter_ndjson_lenient(path)
        return

    dec = json.JSONDecoder()
    with open_text_auto(path, "rt") as f:
        buf = _read_until_nonspace(f, "", chunk_size)
        if not buf:
            return
        first = buf[0]
        if first == "{":
            obj = json.loads(buf + f.read())
            yield _coerce_obj_to_record(obj)
            return
        if first != "[":
            raise ValueError("Top-level JSON must be '[' or '{'.")
        yield from _iter_array_stream(f, buf[1:], dec, chunk_size)



def read_json_bulk_chunks(path: Path, *, chunk_size: int = 10000) -> Iterator[pd.DataFrame]:
    """Yield pandas DataFrame *chunks* from a single JSON/NDJSON file.
    Each row contains the nested record in column 'record'.
    """
    buf: List[Dict[str, Any]] = []
    for rec in read_json_row(path):
        buf.append(rec)
        if len(buf) >= chunk_size:
            yield pd.DataFrame({"record": buf})
            buf = []
    if buf:
        yield pd.DataFrame({"record": buf})



def _flatten(prefix: str, value: Any) -> Iterable[Tuple[str, Any]]:
    if isinstance(value, dict):
        # single-key dict that maps to list -> collapse to key[index]
        if len(value) == 1:
            (k, v), = value.items()
            if isinstance(v, list) and prefix:
                for idx, item in enumerate(v):
                    base = f"{prefix}[{idx}]"
                    if isinstance(item, (dict, list)):
                        for sub_k, sub_v in _flatten("", item):
                            yield f"{base}.{sub_k}" if sub_k else base, sub_v
                    else:
                        yield base, item
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



def append_ndjson_record(path: Path, record: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open_text_auto(path, "at") as f:
        f.write(json.dumps(record, ensure_ascii=False))
        f.write("\n")


def dump_ndjson_records(path: Path, records: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open_text_auto(path, "wt") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False))
            f.write("\n")


def dump_json_records(path: Path, records: List[Dict[str, Any]], indent: int = 2) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open_text_auto(path, "wt") as f:
        json.dump(records, f, indent=indent, ensure_ascii=False)


def dump_records_auto(path: Path, records: List[Dict[str, Any]], indent: int = 2) -> None:
    if is_ndjson_path(path):
        dump_ndjson_records(path, records)
    else:
        dump_json_records(path, records, indent=indent)

