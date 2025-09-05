from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Tuple


@dataclass(frozen=True)
class Out:
    """
    Envelope for multi-output routing.
    - port: name of the output port (e.g., "left", "right", "errors", "*")
    - payload: the actual data (row, DataFrame, dask frame, etc.)
    """

    port: str
    payload: Any


@dataclass(frozen=True)
class InTagged:
    """
    Envelope for multi-input components.
    """

    in_port: str
    payload: Any


def unwrap(obj: Any, default_port: str) -> Tuple[str, Any]:
    """
    Unwrap InTagged envelope to get payloads
    """
    if isinstance(obj, InTagged):
        return obj.in_port, obj.payload
    return default_port, obj
