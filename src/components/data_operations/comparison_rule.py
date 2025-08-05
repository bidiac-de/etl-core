from typing import Any, Literal, List, Optional
from pydantic import BaseModel, Field


class ComparisonRule(BaseModel):
    """Single or nested rule for filtering."""

    column: Optional[str] = Field(None, description="Column to apply the filter on")
    operator: Optional[Literal["==", "!=", ">", "<", ">=", "<=", "contains"]] = None
    value: Optional[Any] = None

    logical_operator: Optional[Literal["AND", "OR", "NOT"]] = None
    rules: Optional[List["ComparisonRule"]] = None

    class Config:
        arbitrary_types_allowed = True
        extra = "ignore"