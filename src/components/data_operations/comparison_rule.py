from __future__ import annotations
from typing import Any, Literal, List, Optional
from pydantic import BaseModel, Field, ConfigDict, model_validator


class ComparisonRule(BaseModel):
    """Single or nested filter rule."""
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

    column: Optional[str] = Field(default=None, description="Column to apply the rule to")
    operator: Optional[Literal["==", "!=", ">", "<", ">=", "<=", "contains"]] = None
    value: Optional[Any] = None

    logical_operator: Optional[Literal["AND", "OR", "NOT"]] = None
    rules: Optional[List["ComparisonRule"]] = None

    @model_validator(mode="after")
    def _check_either_leaf_or_node(self) -> "ComparisonRule":
        is_leaf = self.column is not None or self.operator is not None or self.value is not None
        is_node = self.logical_operator is not None or self.rules is not None

        if is_leaf and is_node:
            raise ValueError("A rule cannot be both a leaf and a logical node.")
        if not is_leaf and not is_node:
            raise ValueError("A rule must be either a leaf rule or a logical node rule.")

        if is_leaf:
            if not (self.column and self.operator is not None):
                raise ValueError("A leaf rule requires both 'column' and 'operator'.")
        if self.logical_operator == "NOT":
            if not (self.rules and len(self.rules) == 1):
                raise ValueError("'NOT' requires exactly one sub-rule.")
        if self.logical_operator in ("AND", "OR"):
            if not (self.rules and len(self.rules) >= 1):
                raise ValueError("'AND'/'OR' requires at least one sub-rule.")
        return self