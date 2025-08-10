from __future__ import annotations
from typing import Any, Dict
import operator
import pandas as pd

from src.components.data_operations.comparison_rule import ComparisonRule

_OPS = {
    "==": operator.eq,
    "!=": operator.ne,
    ">": operator.gt,
    "<": operator.lt,
    ">=": operator.ge,
    "<=": operator.le,
}

def _eval_leaf_on_value(value: Any, op: str, expected: Any) -> bool:
    """Evaluate a single (leaf) condition on a value."""
    if op == "contains":
        try:
            return str(expected).lower() in str(value).lower()
        except Exception:
            return False
    func = _OPS.get(op)
    if func is None:
        return False
    try:
        return bool(func(value, expected))
    except Exception:
        return False


def eval_rule_on_row(row: Dict[str, Any], rule: ComparisonRule) -> bool:
    """Evaluate a rule against a single row represented as a dict."""
    if rule.logical_operator:
        lop = rule.logical_operator
        sub = rule.rules or []
        if lop == "NOT":
            return not eval_rule_on_row(row, sub[0])
        if lop == "AND":
            return all(eval_rule_on_row(row, r) for r in sub)
        if lop == "OR":
            return any(eval_rule_on_row(row, r) for r in sub)
        return False

    val = row.get(rule.column, None)
    return _eval_leaf_on_value(val, rule.operator, rule.value)


def eval_rule_on_frame(pdf: pd.DataFrame, rule: ComparisonRule) -> pd.Series:
    """Return a boolean mask (same index length as the DataFrame)."""
    if pdf.empty:
        return pd.Series([], dtype=bool, index=pdf.index)

    if rule.logical_operator:
        sub = rule.rules or []
        if rule.logical_operator == "NOT":
            return ~eval_rule_on_frame(pdf, sub[0])
        if rule.logical_operator == "AND":
            mask = pd.Series(True, index=pdf.index)
            for r in sub:
                mask &= eval_rule_on_frame(pdf, r)
            return mask
        if rule.logical_operator == "OR":
            mask = pd.Series(False, index=pdf.index)
            for r in sub:
                mask |= eval_rule_on_frame(pdf, r)
            return mask
        return pd.Series(False, index=pdf.index)

    col = rule.column
    if col not in pdf.columns:
        return pd.Series(False, index=pdf.index)

    if rule.operator == "contains":
        return pdf[col].astype(str).str.contains(str(rule.value), case=False, na=False)
    func = _OPS.get(rule.operator)
    if func is None:
        return pd.Series(False, index=pdf.index)
    try:
        return func(pdf[col], rule.value)  # type: ignore
    except Exception:
        return pd.Series(False, index=pdf.index)