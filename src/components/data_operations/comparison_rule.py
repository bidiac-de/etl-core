from typing import Any, Literal
from pydantic import BaseModel, Field

class ComparisonRule(BaseModel):
    """Rule for filtering: column, operator, value."""

    column: str = Field(..., description="Column to apply the filter on")
    operator: Literal["==", "!=", ">", "<", ">=", "<="] = Field(
        ..., description="Comparison operator"
    )
    value: Any = Field(..., description="Value to compare against")