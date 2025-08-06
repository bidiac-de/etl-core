from pathlib import Path
from typing import IO

def resolve_file_path(filepath: Path) -> Path:
    """Resolve a relative or user path and return an absolute path."""
    return filepath.expanduser().resolve()

def ensure_exists(filepath: Path):
    """Ensure that the given file exists."""
    if not filepath.exists():
        raise FileNotFoundError(f"File {filepath} does not exist.")

def open_file(path: Path, mode: str = "r", encoding: str = "utf-8") -> IO:
    """Open a file with consistent default settings."""
    return open(path, mode=mode, newline="", encoding=encoding)

def file_exists(path: Path) -> bool:
    """Check if a file exists."""
    return path.exists()