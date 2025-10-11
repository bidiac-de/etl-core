from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

import etl_core.receivers.data_operations_receivers.filter.filter_helper as FH
from etl_core.components.data_operations.filter.comparison_rule import ComparisonRule
from pydantic import ValidationError


def test__freeze_for_token_dict_set_list_tuple_variants() -> None:
    x = {
        "b": {3, 1, 2},
        "a": [("k", {"z": 1})],
    }
    frozen = FH._freeze_for_token(x)
    assert isinstance(frozen, tuple)
    # structure: ('a', (('k', (('z', 1),)),)), ('b', (1, 2, 3))
    assert ("a", (("k", (("z", 1),)),)) in frozen
    assert ("b", (1, 2, 3)) in frozen


def test__freeze_rule_and_normalize_token_include_children() -> None:
    inner = ComparisonRule(column="x", operator="==", value=1)
    outer = ComparisonRule(logical_operator="NOT", rules=[inner])
    frozen = FH._freeze_rule(outer)
    assert frozen[0] == "ComparisonRule"
    assert frozen[-1] and isinstance(frozen[-1], tuple)


def test__ensure_string_falls_back_when_astype_string_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    s = pd.Series([1, None, 3])

    original = pd.Series.astype

    def boom(self, dtype, *a, **k):  # type: ignore[no-redef]
        if dtype == "string":
            raise TypeError("nope")
        return original(self, dtype, *a, **k)

    monkeypatch.setattr(pd.Series, "astype", boom, raising=True)
    out = FH._ensure_string(s)
    assert out.dtype == object
    assert list(out) == ["1.0", "nan", "3.0"]


def test__leaf_mask_errors_and_all_operators() -> None:
    df = pd.DataFrame({"a": [1, 2, 3], "s": ["aa", "bb", None]})

    with pytest.raises(ValidationError, match="requires both 'column' and 'operator'"):
        _ = ComparisonRule(column=None, operator="==", value=1)

    with pytest.raises(KeyError):
        FH._leaf_mask(df, ComparisonRule(column="missing", operator="==", value=1))

    m = FH._leaf_mask(df, ComparisonRule(column="a", operator="==", value=[1, 3]))
    assert list(df[m]["a"]) == [1, 3]

    m = FH._leaf_mask(df, ComparisonRule(column="s", operator="contains", value=None))
    assert list(df[m]["s"]) == ["aa", "bb"]  # None excluded

    # scalar ops
    assert (
        FH._leaf_mask(df, ComparisonRule(column="a", operator="==", value=2)).sum() == 1
    )
    assert (
        FH._leaf_mask(df, ComparisonRule(column="a", operator="!=", value=2)).sum() == 2
    )
    assert (
        FH._leaf_mask(df, ComparisonRule(column="a", operator=">", value=2)).sum() == 1
    )
    assert (
        FH._leaf_mask(df, ComparisonRule(column="a", operator="<", value=2)).sum() == 1
    )
    assert (
        FH._leaf_mask(df, ComparisonRule(column="a", operator=">=", value=2)).sum() == 2
    )
    assert (
        FH._leaf_mask(df, ComparisonRule(column="a", operator="<=", value=2)).sum() == 2
    )

    # unknown operator:
    fake_bad_op = SimpleNamespace(
        column="a",
        operator="???",
        value=0,
        logical_operator=None,
        rules=None,
    )
    with pytest.raises(ValueError, match="Unknown operator"):
        FH._leaf_mask(df, fake_bad_op)  # type: ignore[arg-type]


def test__all_same_column_all_equal_and_none_cases() -> None:
    c1 = ComparisonRule(column="x", operator="==", value=1)
    c2 = ComparisonRule(column="x", operator="!=", value=2)
    assert FH._all_same_column([c1, c2]) == "x"

    fake = SimpleNamespace(
        column=None, operator="==", rules=None, logical_operator=None
    )
    assert FH._all_same_column([fake]) is None

    c3 = ComparisonRule(column="y", operator="==", value=1)
    assert FH._all_same_column([c1, c3]) is None


def test__optimize_or_eq_and_contains_success_and_nones() -> None:
    df = pd.DataFrame({"x": ["alfa", "bravo", "charlie"]})
    eq_children_ok = [
        ComparisonRule(column="x", operator="==", value="alfa"),
        ComparisonRule(column="x", operator="==", value="charlie"),
    ]
    mask = FH._optimize_or_eq(df, eq_children_ok)
    assert list(df[mask]["x"]) == ["alfa", "charlie"]

    contains_children_ok = [
        ComparisonRule(column="x", operator="contains", value="al"),
        ComparisonRule(column="x", operator="contains", value="lie"),
    ]
    mask2 = FH._optimize_or_contains(df, contains_children_ok)
    assert list(df[mask2]["x"]) == ["alfa", "charlie"]

    assert FH._optimize_or_eq(df, []) is None
    nested = ComparisonRule(logical_operator="OR", rules=[eq_children_ok[0]])
    assert FH._optimize_or_eq(df, [nested]) is None
    mixed_col = [
        ComparisonRule(column="x", operator="==", value="alfa"),
        ComparisonRule(column="y", operator="==", value="alfa"),
    ]
    assert FH._optimize_or_eq(df, mixed_col) is None
    not_all_eq = [
        ComparisonRule(column="x", operator="==", value="alfa"),
        ComparisonRule(column="x", operator="!=", value="alfa"),
    ]
    assert FH._optimize_or_eq(df, not_all_eq) is None

    not_all_contains = [
        ComparisonRule(column="x", operator="contains", value="al"),
        ComparisonRule(column="x", operator="==", value="al"),
    ]
    assert FH._optimize_or_contains(df, not_all_contains) is None


def test__optimize_and_neq_success_and_nones() -> None:
    df = pd.DataFrame({"x": [1, 2, 3, 4]})
    children_ok = [
        ComparisonRule(column="x", operator="!=", value=1),
        ComparisonRule(column="x", operator="!=", value=4),
    ]
    mask = FH._optimize_and_neq(df, children_ok)
    assert list(df[mask]["x"]) == [2, 3]

    assert FH._optimize_and_neq(df, []) is None
    nested = ComparisonRule(logical_operator="AND", rules=[children_ok[0]])
    assert FH._optimize_and_neq(df, [nested]) is None
    wrong_col = [
        ComparisonRule(column="x", operator="!=", value=1),
        ComparisonRule(column="y", operator="!=", value=4),
    ]
    assert FH._optimize_and_neq(df, wrong_col) is None
    wrong_op = [
        ComparisonRule(column="x", operator="==", value=1),
        ComparisonRule(column="x", operator="!=", value=4),
    ]
    assert FH._optimize_and_neq(df, wrong_op) is None


def test__reduce_masks_and_try_optimizers() -> None:
    m1 = pd.Series([True, False, True])
    m2 = pd.Series([True, True, False])
    or_mask = FH._reduce_masks([m1, m2], lambda a, b: a | b)
    and_mask = FH._reduce_masks([m1, m2], lambda a, b: a & b)
    assert list(or_mask) == [True, True, True]
    assert list(and_mask) == [True, False, False]

    with pytest.raises(ValueError, match="empty sequence"):
        FH._reduce_masks([], lambda a, b: a | b)  # type: ignore[arg-type]

    df = pd.DataFrame({"x": [1, 2, 3]})

    def opt_none(_df, _children):
        return None

    def opt_ok(_df, _children):
        return pd.Series([True, False, True])

    assert FH._try_optimizers(df, [], (opt_none,)) is None
    assert list(FH._try_optimizers(df, [], (opt_none, opt_ok))) == [True, False, True]


def test_build_mask_variants_and_errors() -> None:
    df = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 19]})

    rule_leaf = ComparisonRule(column="age", operator=">=", value=25)
    assert list(df[FH.build_mask(df, rule_leaf)]["name"]) == ["Alice", "Bob"]

    rule_or = ComparisonRule(
        logical_operator="OR",
        rules=[
            ComparisonRule(column="name", operator="==", value="Alice"),
            ComparisonRule(column="name", operator="==", value="Charlie"),
        ],
    )
    assert list(df[FH.build_mask(df, rule_or)]["name"]) == ["Alice", "Charlie"]

    rule_or_reduce = ComparisonRule(
        logical_operator="OR",
        rules=[
            ComparisonRule(column="name", operator="contains", value="li"),
            ComparisonRule(column="age", operator=">=", value=30),
        ],
    )
    mask_or_reduce = FH.build_mask(df, rule_or_reduce)
    assert list(df[mask_or_reduce]["name"]) == ["Alice", "Bob", "Charlie"]

    rule_and_opt = ComparisonRule(
        logical_operator="AND",
        rules=[
            ComparisonRule(column="age", operator="!=", value=19),
            ComparisonRule(column="age", operator="!=", value=25),
        ],
    )
    assert list(df[FH.build_mask(df, rule_and_opt)]["name"]) == ["Bob"]

    rule_and_reduce = ComparisonRule(
        logical_operator="AND",
        rules=[
            ComparisonRule(column="age", operator=">=", value=20),
            ComparisonRule(column="name", operator="contains", value="li"),
        ],
    )
    mask_and_reduce = FH.build_mask(df, rule_and_reduce)
    assert list(df[mask_and_reduce]["name"]) == ["Alice"]

    rule_not = ComparisonRule(
        logical_operator="NOT",
        rules=[ComparisonRule(column="name", operator="contains", value="li")],
    )
    assert list(df[FH.build_mask(df, rule_not)]["name"]) == ["Bob"]

    with pytest.raises(ValidationError, match="requires at least one sub-rule"):
        _ = ComparisonRule(logical_operator="OR", rules=[])

    with pytest.raises(ValidationError, match="requires exactly one sub-rule"):
        _ = ComparisonRule(
            logical_operator="NOT",
            rules=[
                ComparisonRule(column="age", operator=">=", value=20),
                ComparisonRule(column="age", operator="<", value=40),
            ],
        )


    fake_xor = SimpleNamespace(
        logical_operator="XOR",
        rules=[rule_leaf],
        column=None,
        operator=None,
        value=None,
    )
    with pytest.raises(ValueError, match="Unknown logical operator"):
        FH.build_mask(df, fake_xor)  # type: ignore[arg-type]


def test__leaf_mask_col_none_branch() -> None:
    df = pd.DataFrame({"a": [1, 2, 3]})
    fake_rule = SimpleNamespace(
        column=None,
        operator="==",
        value=1,
        logical_operator=None,
        rules=None,
    )
    with pytest.raises(ValueError, match="Leaf rule requires 'column'"):
        FH._leaf_mask(df, fake_rule)  # type: ignore[arg-type]


def test__optimize_or_contains_empty_children_branch() -> None:
    df = pd.DataFrame({"x": ["a", "b"]})
    result = FH._optimize_or_contains(df, [])
    assert result is None


def test_build_mask_not_enough_children_branches() -> None:
    df = pd.DataFrame({"x": [1]})

    fake_or = SimpleNamespace(
        logical_operator="OR",
        rules=[],
        column=None,
        operator=None,
        value=None,
    )
    with pytest.raises(ValueError, match="requires at least one child rule"):
        FH.build_mask(df, fake_or)  # type: ignore[arg-type]

    fake_not = SimpleNamespace(
        logical_operator="NOT",
        rules=[SimpleNamespace(column="x", operator="==", value=1)] * 2,
        column=None,
        operator=None,
        value=None,
    )
    with pytest.raises(ValueError, match="requires exactly one child rule"):
        FH.build_mask(df, fake_not)  # type: ignore[arg-type]
