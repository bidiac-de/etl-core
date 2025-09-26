from __future__ import annotations

import sys
import types

import pandas as pd
import pytest

from etl_core.components.data_operations.filter.comparison_rule import ComparisonRule


def test_leaf_and_node_together_raises() -> None:
    with pytest.raises(ValueError):
        ComparisonRule(
            column="c",
            operator="==",
            value=1,
            logical_operator="AND",
            rules=[],
        )


def test_neither_leaf_nor_node_raises() -> None:
    with pytest.raises(ValueError):
        ComparisonRule()


def test_leaf_missing_column_or_operator_raises() -> None:
    # missing column
    with pytest.raises(ValueError):
        ComparisonRule(operator="==", value=1)
    # missing operator
    with pytest.raises(ValueError):
        ComparisonRule(column="c", value=1)


def test_node_not_requires_exactly_one_subrule() -> None:
    with pytest.raises(ValueError):
        ComparisonRule(logical_operator="NOT", rules=[])
    with pytest.raises(ValueError):
        ComparisonRule(
            logical_operator="NOT",
            rules=[
                ComparisonRule(column="c", operator="==", value=1),
                ComparisonRule(column="d", operator="==", value=2),
            ],
        )


def test_node_and_or_requires_at_least_one() -> None:
    with pytest.raises(ValueError):
        ComparisonRule(logical_operator="AND", rules=[])
    with pytest.raises(ValueError):
        ComparisonRule(logical_operator="OR", rules=None)


def test_to_mask_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    ComparisonRule.to_mask does a *relative import*:
        from .filter_helper import build_mask
    Some repos may not ship that module in test contexts. Inject a dummy
    module into sys.modules at the fully-qualified path so the import works,
    and verify delegation + return value.
    """
    rule = ComparisonRule(column="x", operator="==", value=1)
    df = pd.DataFrame({"x": [1, 2]})

    called: dict[str, bool] = {}

    def fake_build_mask(df_arg, rule_arg):
        called["ok"] = True
        assert df_arg is df and rule_arg is rule
        return pd.Series([True, False])

    # Create a dummy module to satisfy `from .filter_helper import build_mask`
    module_name = "etl_core.components.data_operations.filter.filter_helper"
    dummy_mod = types.ModuleType(module_name)
    dummy_mod.build_mask = fake_build_mask  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module_name, dummy_mod)

    mask = rule.to_mask(df)
    assert called.get("ok") is True
    assert mask.tolist() == [True, False]


def test_filter_df_applies_mask(monkeypatch: pytest.MonkeyPatch) -> None:
    df = pd.DataFrame({"x": [1, 2]})
    rule = ComparisonRule(column="x", operator="==", value=1)

    # Patch the class method (instance attribute patching is restricted by Pydantic)
    monkeypatch.setattr(
        ComparisonRule,
        "to_mask",
        lambda self, frame: pd.Series([True, False]),
        raising=True,
    )

    out = rule.filter_df(df)
    assert list(out["x"]) == [1]
