import gzip
import io
import json
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple


def open_text_auto(path: Path, mode: str = "rt", encoding: str = "utf-8"):
    """
    Open text or gzip compressed text transparently.
    """
    p = str(path)
    if p.endswith(".gz"):
        # gzip.open wants binary mode
        return io.TextIOWrapper(gzip.open(p, mode.replace("t", "")), encoding=encoding)
    return open(path, mode, encoding=encoding)


def is_ndjson_path(path: Path) -> bool:
    """
    Public predicate for NDJSON/JSONL detection (incl. gz).
    """
    p = str(path).lower()
    return p.endswith((".jsonl", ".ndjson", ".jsonl.gz", ".ndjson.gz"))


# Backwards compatibility for any internal uses.
def _is_ndjson_path(p: str) -> bool:  # noqa: D401
    return p.endswith((".jsonl", ".ndjson", ".jsonl.gz", ".ndjson.gz"))


def load_json_records(path: Path) -> List[Dict[str, Any]]:
    """
    Load into memory as list of dicts.

    Supports:
      - JSON array: [ {...}, {...} ]
      - Single JSON object: { ... } -> [ { ... } ]
      - NDJSON (one JSON per line)
    """
    with open_text_auto(path, "rt") as f:
        text = f.read().strip()
    if not text:
        return []
    if text.startswith("["):
        data = json.loads(text)
        return data if isinstance(data, list) else [data]
    # Could be NDJSON or a single object without brackets
    lines = [ln for ln in text.splitlines() if ln.strip()]
    try:
        return [json.loads(ln) for ln in lines]
    except json.JSONDecodeError:
        obj = json.loads(text)
        return obj if isinstance(obj, list) else [obj]


def dump_json_records(path: Path, records: List[Dict[str, Any]], indent: int = 2):
    """
    Write a JSON array (ensure UTF-8, no ASCII escaping).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open_text_auto(path, "wt") as f:
        json.dump(records, f, indent=indent, ensure_ascii=False)


def append_ndjson_record(path: Path, record: Dict[str, Any]) -> None:
    """
    Append a single record to an NDJSON file efficiently.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open_text_auto(path, "at") as f:
        f.write(json.dumps(record, ensure_ascii=False))
        f.write("\n")


def dump_ndjson_records(path: Path, records: List[Dict[str, Any]]) -> None:
    """
    Write many records as NDJSON.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open_text_auto(path, "wt") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False))
            f.write("\n")


def dump_records_auto(path: Path, records: List[Dict[str, Any]], indent: int = 2):
    """
    Write records as NDJSON if path indicates NDJSON, else a JSON array.
    """
    if is_ndjson_path(path):
        dump_ndjson_records(path, records)
    else:
        dump_json_records(path, records, indent=indent)


def _append_next_chunk(buf: str, f, size: int) -> Tuple[str, bool]:
    more = f.read(size)
    return (buf + more, False) if more else (buf, True)


def _detect_top_level(buf: str) -> Optional[str]:
    i = 0
    while i < len(buf) and buf[i].isspace():
        i += 1
    if i >= len(buf):
        return None
    if buf[i] == "[":
        return "array"
    if buf[i] == "{":
        return "object"
    raise ValueError("Top-level JSON must be '[' or '{'.")


def _skip_seps(buf: str, i: int = 0) -> int:
    while i < len(buf) and (buf[i].isspace() or buf[i] == ","):
        i += 1
    return i


def _try_decode(dec: json.JSONDecoder, buf: str, i: int):
    try:
        return dec.raw_decode(buf, idx=i)
    except json.JSONDecodeError:
        return None


def _iter_ndjson(path: Path) -> Iterator[Dict[str, Any]]:
    with open_text_auto(path, "rt") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            obj = json.loads(s)
            yield obj if isinstance(obj, dict) else {"_value": obj}


def _iter_single_object(
    dec: json.JSONDecoder, buf: str, f, size: int, i: int
) -> Iterator[Dict[str, Any]]:
    while True:
        d = _try_decode(dec, buf, i)
        if d:
            obj, _end = d
            yield obj if isinstance(obj, dict) else {"_value": obj}
            return
        buf, eof = _append_next_chunk(buf, f, size)
        if eof:
            raise ValueError("Unexpected EOF in single JSON object.")


def _need_more_data(buf: str, j: int) -> bool:
    """
    True if there is no next token after skipping whitespace/commas.
    """
    return j >= len(buf)


def _fetch_or_close(buf: str, f, size: int) -> Tuple[str, bool]:
    """
    Read next chunk; if EOF and buffer empty -> cleanly finished.
    """
    buf, eof = _append_next_chunk(buf, f, size)
    if eof and not buf.strip():
        return buf, True
    return buf, False


def _ensure_token(buf: str, f, size: int) -> Tuple[Optional[int], str]:
    """
    Ensure next token available; return (index, buf). index=None -> done.
    """
    while True:
        j = _skip_seps(buf, 0)
        if not _need_more_data(buf, j):
            return j, buf
        buf, closed = _fetch_or_close(buf, f, size)
        if closed:
            return None, buf


def _decode_or_read(
    dec: json.JSONDecoder, buf: str, start: int, f, size: int
) -> Tuple[Dict[str, Any], str]:
    """
    Decode next object from buffer, reading more until complete.
    """
    while True:
        decoded = _try_decode(dec, buf, start)
        if decoded:
            obj, end = decoded
            if isinstance(obj, dict):
                return obj, buf[end:]
            return {"_value": obj}, buf[end:]
        buf, eof = _append_next_chunk(buf, f, size)
        if eof:
            raise ValueError("JSON array not properly closed.")


def _iter_array(
    dec: json.JSONDecoder, buf: str, f, size: int
) -> Iterator[Dict[str, Any]]:
    while True:
        j, buf = _ensure_token(buf, f, size)
        if j is None:
            return
        if buf[j] == "]":
            return
        obj, buf = _decode_or_read(dec, buf, j, f, size)
        yield obj


def _prime_top_level(f, size: int, buf: str) -> Tuple[Optional[str], str, int]:
    """
    Read chunks until top-level token is detected or EOF.
    """
    while True:
        buf, eof = _append_next_chunk(buf, f, size)
        if eof and not buf:
            return None, "", 0
        top = _detect_top_level(buf)
        if top is None:
            if eof:
                return None, "", 0
            buf = buf.lstrip()
            continue
        break

    i = 0
    while i < len(buf) and buf[i].isspace():
        i += 1
    return top, buf, i


def read_json_row(path: Path, chunk_size: int = 65536) -> Iterator[Dict[str, Any]]:
    """
    Streaming iterator over JSON content.

    - NDJSON: yields per line
    - Single object: yields once
    - Array: yields per element
    """
    if is_ndjson_path(path):
        yield from _iter_ndjson(path)
        return

    dec = json.JSONDecoder()
    buf = ""
    with open_text_auto(path, "rt") as f:
        top, buf, i = _prime_top_level(f, chunk_size, buf)
        if top is None:
            return

        if top == "object":
            yield from _iter_single_object(dec, buf, f, chunk_size, i)
            return

        if top == "array":
            if i < len(buf) and buf[i] == "[":
                buf = buf[i + 1 :]
            yield from _iter_array(dec, buf, f, chunk_size)
            return

        raise ValueError("Top-level JSON must be '[' or '{'.")
