from pathlib import Path
from typing import IO
from typing import Any


class FileReceiverError(Exception):
    pass


def resolve_file_path(filepath: Path) -> Path:
    """Resolve a relative or user path and return an absolute path."""
    return filepath.expanduser().resolve()


def ensure_file_exists(filepath: Path):
    """
    Ensure that the given file exists (raises FileNotFoundError if not).
    Unified replacement for earlier ensure_exists / file_exists helpers.
    """
    if not filepath.exists():
        raise FileNotFoundError(f"File {filepath} does not exist.")


def open_file(
    path: Path,
    mode: str = "r",
    *,
    encoding: str = "utf-8",
    newline: str | None = "",
    **kwargs: Any,
) -> IO:
    """Open a file with consistent defaults and allow extra args like newline."""
    if "b" in mode:
        return open(path, mode=mode, **kwargs)
    return open(path, mode=mode, encoding=encoding, newline=newline, **kwargs)
