import gzip
import io
import json
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional
import re
import pandas as pd
import dask.dataframe as dd


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


def iter_ndjson_lenient(
    path: Path,
    on_error: Optional[Callable[[Exception], None]] = None,
) -> Iterator[Dict[str, Any]]:
    """
    Iterate NDJSON records but skip malformed lines.
    Non-dict values are wrapped as {"_value": <value>}.
    """
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
            yield obj if isinstance(obj, dict) else {"_value": obj}


def append_ndjson_record(path: Path, record: Dict[str, Any]) -> None:
    """Append a single record to an NDJSON file efficiently."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open_text_auto(path, "at") as f:
        f.write(json.dumps(record, ensure_ascii=False))
        f.write("\n")


def dump_ndjson_records(path: Path, records: List[Dict[str, Any]]) -> None:
    """Write many records as NDJSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open_text_auto(path, "wt") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False))
            f.write("\n")


def load_json_records(path: Path) -> List[Dict[str, Any]]:
    """
    Load into memory as list of dicts.

    Supports:
      - JSON array: [ {...}, {...} ]
      - Single JSON object: { ... }  -> [ { ... } ]
      - NDJSON (one JSON per line)
    """
    if is_ndjson_path(path):
        return list(iter_ndjson_lenient(path))
    with open_text_auto(path, "rt") as f:
        text = f.read().strip()
    if not text:
        return []
    if text.startswith("["):
        data = json.loads(text)
        return data if isinstance(data, list) else [data]
    obj = json.loads(text)
    return obj if isinstance(obj, list) else [obj]


def dump_json_records(
    path: Path, records: List[Dict[str, Any]], indent: int = 2
) -> None:
    """Write a JSON array with UTF-8 (no ASCII escaping)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open_text_auto(path, "wt") as f:
        json.dump(records, f, indent=indent, ensure_ascii=False)


def dump_records_auto(
        path: Path, records: List[Dict[str, Any]], indent: int = 2
) -> None:
    """Write NDJSON if path looks like NDJSON; else JSON-Array."""
    if is_ndjson_path(path):
        dump_ndjson_records(path, records)
    else:
        dump_json_records(path, records, indent=indent)



def _coerce_obj_to_record(obj: Any) -> Dict[str, Any]:
    """Wrap non-dict JSON values to keep a stable record shape."""
    return obj if isinstance(obj, dict) else {"_value": obj}


def _consume_array_separators(s: str) -> str:
    """Trim leading whitespace and commas inside a JSON array."""
    i = 0
    while i < len(s) and (s[i].isspace() or s[i] == ","):
        i += 1
    return s[i:]


def _read_until_nonspace(f: io.TextIOBase, initial: str, chunk_size: int) -> str:
    """
    Keep reading until we see a non-space character or EOF.
    Returns the buffer starting at the first non-space char, or '' if empty.
    """
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
    """
    Yield array elements from an open file handle using incremental decoding.
    `buf` should start after the opening '[' character.
    """
    while True:
        buf = _consume_array_separators(buf)

        # End of array
        if buf.startswith("]"):
            return

        # Need more data
        if not buf.strip():
            more = f.read(chunk_size)
            if not more:
                return
            buf += more
            continue

        # Try to decode one JSON value, read more if incomplete
        try:
            obj, end = dec.raw_decode(buf)
        except json.JSONDecodeError:
            more = f.read(chunk_size)
            if not more:
                raise
            buf += more
            continue

        yield _coerce_obj_to_record(obj)
        buf = buf[end:]


def read_json_row(path: Path, chunk_size: int = 65536) -> Iterator[Dict[str, Any]]:
    """
    Streaming iterator over JSON content.

    Supported inputs:
      - Single object: { ... }        -> yields the object once
      - JSON array:    [ {...}, ... ] -> yields per element
      - Gzipped files are supported via open_text_auto()
      - Non-dict values are wrapped as {"_value": <value>}
    """
    dec = json.JSONDecoder()
    with open_text_auto(path, "rt") as f:
        # Load enough to decide on top-level type
        buf = _read_until_nonspace(f, "", chunk_size)
        if not buf:
            return

        first = buf[0]
        if first == "{":
            # Single object
            obj = json.loads(buf + f.read())
            yield _coerce_obj_to_record(obj)
            return

        if first != "[":
            raise ValueError("Top-level JSON must be '[' or '{'.")

        # Stream array elements after the initial [
        yield from _iter_array_stream(f, buf[1:], dec, chunk_size)

_PATH_RE = re.compile(r"\.?([^\.\[\]]+)(?:\[(\d+)\])?")

def _is_nullish(v: Any) -> bool:
    try:
        import pandas as _pd  # lazy to avoid hard dep at import-time
        if v is None or _pd.isna(v):
            return True
    except Exception:
        if v is None:
            return True
    return False

def _is_flat_key(key: str) -> bool:
    return "." in key or ("[" in key and "]" in key)

def _has_flat_paths(d: Dict[str, Any]) -> bool:
    return any(_is_flat_key(k) for k in d.keys())

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

def _join(prefix: str, key: str) -> str:
    return f"{prefix}.{key}" if prefix else key

def _flatten_to_map(prefix: str, value: Any, out: Dict[str, Any]) -> None:
    if isinstance(value, dict):
        for k, v in value.items():
            _flatten_to_map(_join(prefix, k), v, out)
    elif isinstance(value, list):
        for i, item in enumerate(value):
            _flatten_to_map(f"{prefix}[{i}]", item, out)
    else:
        out[prefix] = value

def flatten_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    flat: Dict[str, Any] = {}
    _flatten_to_map("", rec, flat)
    return {k.lstrip("."): v for k, v in flat.items()}

def build_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    For writes in bulk/bigdata:
      - flat dict with dotted / [i] keys  -> unflatten_record(...)
      - nested dict                       -> passthrough
    Drops null-ish values.
    """
    if not isinstance(payload, dict):
        raise TypeError(f"Expected dict payload, got {type(payload).__name__}: {payload}")
    cleaned = {k: v for k, v in payload.items() if not _is_nullish(v)}
    return unflatten_record(cleaned) if _has_flat_paths(cleaned) else cleaned

def ensure_nested_for_read(payload: Dict[str, Any]) -> Dict[str, Any]:
    """For read_row: keep nested shape; if flat keys detected, unflatten."""
    return unflatten_record(payload) if _has_flat_paths(payload) else payload

def _validate_key_json(key: Any, ctx: str) -> None:
    if not isinstance(key, str):
        where = ctx or "<root>"
        raise ValueError(f"Row mode expects string keys; offending non-string key at '{where}'.")
    if _is_flat_key(key):
        where = ctx or "<root>"
        raise ValueError(
            "Row mode expects nested dicts only (no flat paths). "
            f"Offending key '{key}' at '{where}'."
        )

def _validate_node_json(obj: Any, ctx: str = "") -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            _validate_key_json(k, ctx)
            next_ctx = f"{ctx}.{k}" if ctx else k
            _validate_node_json(v, next_ctx)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            next_ctx = f"{ctx}[{i}]" if ctx else f"[{i}]"
            _validate_node_json(item, next_ctx)
    # primitives are fine


def flatten_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    if pdf is None or pdf.empty:
        return pd.DataFrame()

    records = []
    for _, row in pdf.iterrows():
        obj = row.to_dict()
        if len(obj) == 1 and isinstance(next(iter(obj.values())), dict):
            obj = next(iter(obj.values()))
        records.append(obj)

    flat = pd.json_normalize(records, sep=".")
    return flat



def infer_flat_meta(ddf: dd.DataFrame) -> pd.DataFrame:
    try:
        sample: pd.DataFrame = ddf.head(1000, compute=True)
        if sample is not None and not sample.empty:
            flat = flatten_partition(sample)
            if flat is not None and list(flat.columns):
                return flat.iloc[:0]
    except Exception:
        pass

    try:
        first: pd.DataFrame = ddf.get_partition(0).compute()
        if first is not None and not first.empty:
            flat = flatten_partition(first)
            if flat is not None and list(flat.columns):
                return flat.iloc[:0]
    except Exception:
        pass

    return pd.DataFrame()



