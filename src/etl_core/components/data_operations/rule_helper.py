def ensure_no_empty_path_segments(value: str, *, separator: str = ".") -> str:
    """Validate dotted/segmented paths like 'a.b.c' and return the value unchanged."""
    parts = value.split(separator)
    if any(part.strip() == "" for part in parts):
        raise ValueError(
            "paths must not contain empty segments (e.g., 'a..b' or '.a' or 'a.')"
        )
    return value
