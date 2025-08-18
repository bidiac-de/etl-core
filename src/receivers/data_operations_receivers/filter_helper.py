from __future__ import annotations

import re
from typing import Mapping, Sequence, Union

import pandas as pd

try:
    import dask.dataframe as dd
except Exception:
    dd = None

from src.components.data_operations.filter.comparison_rule import ComparisonRule


def _ensure_string(series: pd.Series) -> pd.Series:
    """Return a robust string view that works for pandas nullable dtypes."""
    try:
        return series.astype("string")
    except Exception:
        return series.astype(str)


def _leaf_mask(
    df: Union[pd.DataFrame, "dd.DataFrame"],
    rule: ComparisonRule,
) -> pd.Series:
    """Evaluate a single leaf rule against a DataFrame and return a boolean mask."""
    col = rule.column
    if col is None:
        raise ValueError("Leaf rule requires 'column'.")
    if col not in df.columns:
        raise KeyError(f"Column '{col}' not found in DataFrame.")

    s = df[col]
    op = rule.operator
    val = rule.value

    if op == "==" and isinstance(val, (list, tuple, set)):
        return s.isin(list(val))

    if op == "contains":
        pat = re.escape("" if val is None else str(val))
        return _ensure_string(s).str.contains(pat, case=False, na=False, regex=True)

    if op == "==":
        return s == val
    if op == "!=":
        return s != val
    if op == ">":
        return s > val
    if op == "<":
        return s < val
    if op == ">=":
        return s >= val
    if op == "<=":
        return s <= val

    raise ValueError(f"Unknown operator: {op}")


def _all_same_column(children: Sequence[ComparisonRule]) -> str | None:
    """Return the common column name if all child rules target the same column."""
    cols = {c.column for c in children}
    if len(cols) == 1:
        col = next(iter(cols))
        return str(col) if col is not None else None
    return None


def _optimize_or_eq(
    df: Union[pd.DataFrame, "dd.DataFrame"],
    children: Sequence[ComparisonRule],
):
    """Optimize OR of many equality checks on one column to a single .isin(...)."""
    if not children or any(c.rules for c in children):
        return None
    col = _all_same_column(children)
    if col is None:
        return None
    if not all(c.operator == "==" for c in children):
        return None
    values = [c.value for c in children]
    return df[col].isin(values)


def _optimize_or_contains(
    df: Union[pd.DataFrame, "dd.DataFrame"],
    children: Sequence[ComparisonRule],
):
    """Optimize OR of many 'contains' checks on one column into a single regex."""
    if not children or any(c.rules for c in children):
        return None
    col = _all_same_column(children)
    if col is None:
        return None
    if not all(c.operator == "contains" for c in children):
        return None
    pat = "|".join(re.escape("" if c.value is None else str(c.value)) for c in children)
    return _ensure_string(df[col]).str.contains(pat, na=False, regex=True)


def _optimize_and_neq(
    df: Union[pd.DataFrame, "dd.DataFrame"],
    children: Sequence[ComparisonRule],
):
    """
    Optimize AND of many inequality checks on one column into a negated .isin(...).
    """
    if not children or any(c.rules for c in children):
        return None
    col = _all_same_column(children)
    if col is None:
        return None
    if not all(c.operator == "!=" for c in children):
        return None
    values = [c.value for c in children]
    return ~df[col].isin(values)


def build_mask(df: Union[pd.DataFrame, "dd.DataFrame"], rule: ComparisonRule):
    """Build a pandas/dask-compatible boolean mask for the given rule."""
    if rule.logical_operator is None:
        return _leaf_mask(df, rule)

    op = rule.logical_operator
    children = rule.rules or []

    if op == "NOT":
        return ~build_mask(df, children[0])

    if op == "OR":
        for opt in (_optimize_or_eq(df, children), _optimize_or_contains(df, children)):
            if opt is not None:
                return opt
        mask = build_mask(df, children[0])
        for r in children[1:]:
            mask = mask | build_mask(df, r)
        return mask

    if op == "AND":
        opt = _optimize_and_neq(df, children)
        if opt is not None:
            mask = opt
        else:
            mask = build_mask(df, children[0])
        for r in children[1:]:
            mask = mask & build_mask(df, r)
        return mask

    raise ValueError(f"Unknown logical operator: {op}")


def eval_rule_on_frame(pdf: pd.DataFrame, rule: ComparisonRule) -> pd.Series:
    """Return a boolean mask for a pandas DataFrame."""
    return build_mask(pdf, rule)


def eval_rule_on_row(
    row: Mapping[str, object] | pd.Series,
    rule: ComparisonRule,
) -> bool:
    """
    Evaluate a single row (dict/Series) against the rule.
    Reuse the DataFrame-based logic by constructing a single-row DataFrame.
    """
    series = row if isinstance(row, pd.Series) else pd.Series(dict(row))
    mask = build_mask(series.to_frame().T, rule)
    return bool(mask.iloc[0])
