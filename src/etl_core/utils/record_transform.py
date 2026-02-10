"""
Centralized record transformation utilities for flattening/unflattening nested dicts.

This module provides the single source of truth for:
- unflatten_record: Convert flat dict with dotted/indexed keys to nested dict
- flatten_record: Convert nested dict to flat dict with dotted/indexed keys
- has_flat_paths: Check if a dict has flat-style keys (dots or brackets)

Used by json_helper, xml_helper, and common_helpers.
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple, Optional

# Special characters that need escaping in keys
_SPECIAL_CHARS = {".", "[", "]", "\\"}


def _escape_key(key: str) -> str:
    """Escape special characters in a key for flattening."""
    out = []
    for ch in str(key):
        if ch in _SPECIAL_CHARS:
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)


def _unescape_key(key: str) -> str:
    """Unescape special characters in a key."""
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


def _parse_path_escaped(path: str) -> List[Tuple[str, Optional[int]]]:
    """
    Parse a flattened path into parts, handling escaped characters.

    Returns list of (name, index) tuples where index is None for dict access
    or an int for list access.

    Examples:
        "a.b" -> [("a", None), ("b", None)]
        "a[0].b" -> [("a", 0), ("b", None)]
        "a\\.b" -> [("a.b", None)]  # escaped dot
    """
    parts: List[List[Any]] = []
    name_buf: List[str] = []
    i = 0

    def flush_name():
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
    return [(name, idx) for (name, idx) in parts if name or idx is not None]


def _is_flat_key(key: str) -> bool:
    """Check if a single key looks like a flattened path."""
    return "." in key or ("[" in key and "]" in key)


def has_flat_paths(d: Dict[str, Any]) -> bool:
    """
    Check if a dict has any keys that look like flattened paths.

    Returns True if any key contains '.' or '[...]' patterns.
    """
    return any(_is_flat_key(k) for k in d.keys())


def _ensure_list(obj: dict, key: str) -> list:
    """Ensure obj[key] is a list, creating it if needed."""
    if key not in obj or not isinstance(obj[key], list):
        obj[key] = []
    return obj[key]


def _ensure_dict(obj: dict, key: str) -> dict:
    """Ensure obj[key] is a dict, creating it if needed."""
    if key not in obj or not isinstance(obj[key], dict):
        obj[key] = {}
    return obj[key]


def _set_path(root: dict, path: str, value: Any) -> None:
    """
    Set a value at a nested path in root dict.

    Handles both dict access (dots) and list access (brackets).
    """
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
                lst.append(None)
            if last:
                lst[j] = value
                return
            next_name, next_idx = parts[i + 1]
            if next_idx is None:
                if lst[j] is None or not isinstance(lst[j], dict):
                    lst[j] = {}
            else:
                if lst[j] is None or not isinstance(lst[j], list):
                    lst[j] = []
            cur = lst[j]


def unflatten_record(flat: Dict[str, Any], *, sep: str = ".") -> Dict[str, Any]:
    """
    Convert a flat dict with dotted/indexed keys into a nested dict.

    This is the canonical implementation used across the codebase.
    Supports:
    - Dotted paths: 'a.b.c' -> {'a': {'b': {'c': value}}}
    - Array indices: 'a[0].b' -> {'a': [{'b': value}]}
    - Escaped characters: 'a\\.b' -> {'a.b': value}

    Args:
        flat: Dict with flattened keys
        sep: Separator character (default '.'), currently only '.' is fully supported
             for array index handling

    Returns:
        Nested dict structure

    Examples:
        >>> unflatten_record({'a.b': 1, 'a.c': 2})
        {'a': {'b': 1, 'c': 2}}

        >>> unflatten_record({'items[0]': 'x', 'items[1]': 'y'})
        {'items': ['x', 'y']}
    """
    out: Dict[str, Any] = {}
    for k, v in flat.items():
        if k:
            _set_path(out, k, v)
    return out


def _join(prefix: str, key: str) -> str:
    """Join prefix and key with dot, escaping special chars in key."""
    ekey = _escape_key(key)
    return f"{prefix}.{ekey}" if prefix else ekey


def _flatten_to_map(prefix: str, value: Any, out: Dict[str, Any]) -> None:
    """Recursively flatten a nested structure into out dict."""
    if isinstance(value, dict):
        for k, v in value.items():
            _flatten_to_map(_join(prefix, str(k)), v, out)
    elif isinstance(value, list):
        for i, item in enumerate(value):
            new_prefix = f"{prefix}[{i}]" if prefix else f"[{i}]"
            _flatten_to_map(new_prefix, item, out)
    else:
        out[prefix] = value


def flatten_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a nested dict to a flat dict with dotted/indexed keys.

    This is the inverse of unflatten_record.

    Args:
        rec: Nested dict structure

    Returns:
        Flat dict with dotted keys for nested dicts and [i] for lists

    Examples:
        >>> flatten_record({'a': {'b': 1, 'c': 2}})
        {'a.b': 1, 'a.c': 2}

        >>> flatten_record({'items': ['x', 'y']})
        {'items[0]': 'x', 'items[1]': 'y'}
    """
    flat: Dict[str, Any] = {}
    _flatten_to_map("", rec, flat)
    return {k.lstrip("."): v for k, v in flat.items()}


# Backwards compatibility alias
_has_flat_paths = has_flat_paths

