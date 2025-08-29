from __future__ import annotations

from typing import List, Literal
from pydantic import BaseModel, Field, field_validator

JoinHow = Literal["inner", "left", "right", "outer"]


class JoinStep(BaseModel):
    """
    One join step in a plan.
    Row mode: left_on/right_on are dotted paths in nested dicts.
    Bulk/BigData: left_on/right_on are (flattened) column names.
    """

    left_port: str = Field(..., min_length=1)
    right_port: str = Field(..., min_length=1)
    left_on: str = Field(..., min_length=1)
    right_on: str = Field(..., min_length=1)
    how: JoinHow = "inner"
    output_port: str = Field(..., min_length=1)

    @field_validator("left_on", "right_on")
    @classmethod
    def _no_empty_segments(cls, v: str) -> str:
        parts = v.split(".")
        if any(not p.strip() for p in parts):
            raise ValueError("join key paths must not contain empty segments")
        return v


class JoinPlan(BaseModel):
    steps: List[JoinStep] = Field(default_factory=list)
