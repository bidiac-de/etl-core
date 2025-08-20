import gzip
import io
import json
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional


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
    """Write NDJSON if path indicates NDJSON; else JSON array."""
    (dump_ndjson_records if is_ndjson_path(path) else dump_json_records)(
        path, records, indent=indent
    )


def read_json_row(path: Path, chunk_size: int = 65536) -> Iterator[Dict[str, Any]]:
    """
    Streaming iterator over JSON content.

    - Single object: yields once
    - Array: yields per element using a small buffer and JSONDecoder.raw_decode
    """
    dec = json.JSONDecoder()
    buf = ""
    with open_text_auto(path, "rt") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk and not buf:
                return
            buf += chunk
            buf_stripped = buf.lstrip()
            if buf_stripped:
                break
            if not chunk:
                return

        first = buf_stripped[0]
        buf = buf_stripped

        if first == "{":
            obj = json.loads(buf + f.read())
            yield obj if isinstance(obj, dict) else {"_value": obj}
            return

        if first != "[":
            raise ValueError("Top-level JSON must be '[' or '{'.")

        if buf[0] == "[":
            buf = buf[1:]

        while True:
            i = 0
            while i < len(buf) and (buf[i].isspace() or buf[i] == ","):
                i += 1
            buf = buf[i:]

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
                    raise
                buf += more
                continue

            yield obj if isinstance(obj, dict) else {"_value": obj}
            buf = buf[end:]
