from __future__ import annotations

from dataclasses import dataclass
from typing import Any


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
