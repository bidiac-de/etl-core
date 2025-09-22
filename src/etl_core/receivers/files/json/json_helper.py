import gzip
import io
import json
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional
import pandas as pd
import math
import tempfile
import os


def _atomic_write_textfile(path: Path, writer: Callable[[Path], None]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", delete=False, dir=str(path.parent), suffix=path.suffix
    ) as tmp:
        tmp_path = Path(tmp.name)
    try:
        writer(tmp_path)
        os.replace(tmp_path, path)
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass


def dump_records_auto(
    path: Path, records: List[Dict[str, Any]], indent: int = 2
) -> None:
    def _write(tmp_path: Path):
        if is_ndjson_path(path):
            dump_ndjson_records(tmp_path, records)  # uses sanitizer
        else:
            dump_json_records(tmp_path, records, indent=indent)  # uses sanitizer

    _atomic_write_textfile(path, _write)


def _to_json_safe_scalar(v):
    try:
        if isinstance(v, float):
            if math.isnan(v) or math.isinf(v):
                return None
            return v
    except Exception:
        pass
    try:
        import pandas as _pd

        if _pd.isna(v):
            return None
    except Exception:
        pass
    return v


def _sanitize_for_json(obj):
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_json(v) for v in obj]
    return _to_json_safe_scalar(obj)


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
    path.parent.mkdir(parents=True, exist_ok=True)
    safe = _sanitize_for_json(record)
    with open_text_auto(path, "at") as f:
        f.write(json.dumps(safe, ensure_ascii=False, allow_nan=False))
        f.write("\n")


def dump_ndjson_records(path: Path, records: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open_text_auto(path, "wt") as f:
        for rec in records:
            safe = _sanitize_for_json(rec)
            f.write(json.dumps(safe, ensure_ascii=False, allow_nan=False))
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
    path.parent.mkdir(parents=True, exist_ok=True)
    safe = _sanitize_for_json(records)
    with open_text_auto(path, "wt") as f:
        json.dump(safe, f, indent=indent, ensure_ascii=False, allow_nan=False)


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

    Supported:
      - Single object: { ... }        -> yields once (incremental parse)
      - JSON array:    [ {...}, ... ] -> yields per element (incremental)
      - Gzipped files via open_text_auto()
      - Non-dict values -> {"_value": <value>}
    """
    dec = json.JSONDecoder()
    with open_text_auto(path, "rt") as f:
        buf = _read_until_nonspace(f, "", chunk_size)
        if not buf:
            return

        first = buf[0]
        if first == "{":
            # incrementally read until full object parsed
            while True:
                try:
                    obj, end = dec.raw_decode(buf)
                    break
                except json.JSONDecodeError:
                    more = f.read(chunk_size)
                    if not more:
                        raise
                    buf += more
            yield _coerce_obj_to_record(obj)
            return

        if first != "[":
            raise ValueError("Top-level JSON must be '[' or '{'.")

        yield from _iter_array_stream(f, buf[1:], dec, chunk_size)


def _is_flat_key(key: str) -> bool:
    return "." in key or ("[" in key and "]" in key)


def _has_flat_paths(d: Dict[str, Any]) -> bool:
    return any(_is_flat_key(k) for k in d.keys())


def _ensure_list(obj, key):
    if key not in obj or not isinstance(obj[key], list):
        obj[key] = []
    return obj[key]


def _ensure_dict(obj, key):
    if key not in obj or not isinstance(obj[key], dict):
        obj[key] = {}
    return obj[key]


def _set_path(root: dict, path: str, value):
    parts = _parse_path_escaped(path)
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
    ekey = _escape_key(key)
    return f"{prefix}.{ekey}" if prefix else ekey


def flatten_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    flat: Dict[str, Any] = {}
    _flatten_to_map("", rec, flat)
    return {k.lstrip("."): v for k, v in flat.items()}


def build_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise TypeError(
            f"Expected dict payload, got {type(payload).__name__}: {payload}"
        )
    if _has_flat_paths(payload):
        return unflatten_record(payload)
    return payload


def ensure_nested_for_read(payload: Dict[str, Any]) -> Dict[str, Any]:
    """For read_row: keep nested shape; if flat keys detected, unflatten."""
    return unflatten_record(payload) if _has_flat_paths(payload) else payload


def _flatten_to_map(prefix: str, value: Any, out: Dict[str, Any]) -> None:
    if isinstance(value, dict):
        for k, v in value.items():
            _flatten_to_map(_join(prefix, str(k)), v, out)
    elif isinstance(value, list):
        for i, item in enumerate(value):
            new_prefix = f"{prefix}[{i}]" if prefix else f"[{i}]"
            _flatten_to_map(new_prefix, item, out)
    else:
        out[prefix] = value


def _flatten_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    if pdf.empty:
        return pdf
    rows: List[Dict[str, Any]] = []
    for _, sr in pdf.iterrows():
        flat: Dict[str, Any] = {}
        for col, v in sr.items():
            if isinstance(v, (dict, list)):
                _flatten_to_map(str(col), v, flat)
            else:
                flat[str(col)] = v
        rows.append(flat)
    return pd.DataFrame.from_records(rows)


def _write_part_ndjson(pdf: pd.DataFrame, path: str) -> int:
    """Executed by Dask as a delayed task."""
    records = [build_payload(r) for r in pdf.to_dict(orient="records")]
    dump_ndjson_records(Path(path), records)
    return len(records)


_SPECIAL_CHARS = {".", "[", "]", "\\"}


def _escape_key(key: str) -> str:
    out = []
    for ch in str(key):
        if ch in _SPECIAL_CHARS:
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)


def _unescape_key(key: str) -> str:
    out = []
    i = 0
    while i < len(key):
        if key[i] == "\\" and i + 1 < len(key):
            out.append(key[i + 1])
            i += 2
        else:
            out.append(key[i])
            i += 1
    return "".join(out)


def _parse_path_escaped(path: str):
    parts = []
    name_buf = []
    i = 0

    def flush_name():
        # append name-part if there is content or if parts empty (to anchor)
        if name_buf or not parts:
            parts.append([_unescape_key("".join(name_buf)), None])
            name_buf.clear()

    while i < len(path):
        c = path[i]
        if c == "\\" and i + 1 < len(path):
            # keep escaped literal
            name_buf.append(path[i + 1])
            i += 2
            continue

        if c == ".":
            flush_name()
            i += 1
            continue

        if c == "[":
            # try parse numeric index
            j = i + 1
            k = j
            while k < len(path) and path[k].isdigit():
                k += 1
            if k > j and k < len(path) and path[k] == "]":
                # finalize current name and set index
                flush_name()
                parts[-1][1] = int(path[j:k])
                i = k + 1
                continue
            # else: literal '[' in name
            name_buf.append("[")
            i += 1
            continue

        name_buf.append(c)
        i += 1

    flush_name()
    return [(name, idx) for (name, idx) in parts if name != "" or idx is not None]


def stream_json_array_to_ndjson(
    src: Path,
    dst: Path,
    on_error: Optional[Callable[[Exception], None]] = None,
    chunk_size: int = 65536,
) -> int:
    count = 0
    dec = json.JSONDecoder()
    with open_text_auto(src, "rt") as f:
        buf = _read_until_nonspace(f, "", chunk_size)
        if not buf:
            return 0
        if buf[0] != "[":
            raise ValueError("Source is not a JSON array.")
        for rec in _iter_array_stream(f, buf[1:], dec, chunk_size):
            try:
                append_ndjson_record(dst, _coerce_obj_to_record(rec))
                count += 1
            except Exception as exc:
                if on_error:
                    on_error(exc)
    return count
