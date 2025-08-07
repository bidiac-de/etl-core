from pathlib import Path
from typing import Union


def resolve_path(filepath: Union[str, Path]) -> Path:
    """
    Resolves a given filepath into a Path object.
    """
    if not filepath:
        raise ValueError("A valid filepath must be provided.")
    path = Path(filepath).expanduser().resolve()
    return path


def file_exists(filepath: Union[str, Path]) -> bool:
    """
    Checks if the given filepath exists.
    """
    try:
        path = resolve_path(filepath)
        return path.exists()
    except Exception:
        return False


def ensure_directory(filepath: Union[str, Path]) -> None:
    """
    Ensures the directory for a given file path exists.
    Creates parent directories if they are missing.
    """
    path = resolve_path(filepath)
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)