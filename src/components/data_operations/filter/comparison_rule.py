from __future__ import annotations

from typing import Any, List, Optional, Union, Literal

import pandas as pd

try:
    import dask.dataframe as dd
except Exception:
    dd = None

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ComparisonRule(BaseModel):
    """
    Single or nested filter rule.

    A rule is either:
    - a *leaf* rule (column/operator/value), or
    - a *logical node* (AND / OR / NOT) with sub-rules.

    The actual evaluation (mask building) is implemented in `filter_helper.py`.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

    column: Optional[str] = Field(
        default=None,
        description="Column to apply the rule to.",
    )
    operator: Optional[Literal["==", "!=", ">", "<", ">=", "<=", "contains"]] = None
    value: Optional[Any] = None

    # Logical node fields
    logical_operator: Optional[Literal["AND", "OR", "NOT"]] = None
    rules: Optional[List["ComparisonRule"]] = None

    @model_validator(mode="after")
    def _check_either_leaf_or_node(self) -> "ComparisonRule":
        """
        Validate that the instance is either a leaf or a logical node, not both or neither.
        Also validate shape constraints for the chosen kind.
        """
        is_leaf = any(
            [
                self.column is not None,
                self.operator is not None,
                self.value is not None,
                ],
        )
        is_node = any([self.logical_operator is not None, self.rules is not None])

        if is_leaf and is_node:
            raise ValueError("A rule cannot be both a leaf and a logical node.")
        if not is_leaf and not is_node:
            raise ValueError("A rule must be either a leaf rule or a logical node rule.")

        if is_leaf:
            self._validate_leaf_rule()
        else:
            self._validate_node_rule()
        return self

    def _validate_leaf_rule(self) -> None:
        """Leaf rules require both 'column' and 'operator'."""
        if not (self.column and self.operator is not None):
            raise ValueError("A leaf rule requires both 'column' and 'operator'.")

    def _validate_node_rule(self) -> None:
        """Logical-node rules must respect operator arity (NOT=1, AND/OR>=1)."""
        if self.logical_operator == "NOT":
            if not (self.rules and len(self.rules) == 1):
                raise ValueError("'NOT' requires exactly one sub-rule.")
        elif self.logical_operator in ("AND", "OR"):
            if not (self.rules and len(self.rules) >= 1):
                raise ValueError("'AND'/'OR' requires at least one sub-rule.")


    def to_mask(self, df: Union[pd.DataFrame, "dd.DataFrame"]):
        """Build a pandas/dask boolean mask for this rule."""
        from .filter_helper import build_mask  # type: ignore

        return build_mask(df, self)

    def filter_df(self, df: Union[pd.DataFrame, "dd.DataFrame"]):
        """Apply this rule directly to a (pandas/dask) DataFrame."""
        return df[self.to_mask(df)]