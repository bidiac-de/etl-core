from __future__ import annotations

from enum import Enum
from typing import Any, Dict

from pydantic import BaseModel, Field


class AggregationOp(str, Enum):
    """Supported aggregation operations for group-by."""

    COUNT = "count"
    SUM = "sum"
    MIN = "min"
    MAX = "max"
    MEAN = "mean"
    MEDIAN = "median"
    STD = "std"
    NUNIQUE = "nunique"


class AggOp(BaseModel):
    """One aggregation instruction."""

    src: str = Field(..., description="Source field (or '*' for row count)")
    op: AggregationOp = Field(..., description="Aggregation operation")
    dest: str = Field(..., description="Destination field name")

    def to_dict(self) -> Dict[str, Any]:
        # Keep Enum value intact for downstream receivers
        return {"src": self.src, "op": self.op, "dest": self.dest}
