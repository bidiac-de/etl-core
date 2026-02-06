from __future__ import annotations

from typing import Any, Dict, List

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from etl_core.components.wiring.column_definition import DataType, FieldDef
from etl_core.components.wiring.schema import Schema
from etl_core.receivers.data_operations_receivers.type_conversion import (
    type_conversion_helper as H,
)


def test__parse_path_and_pd_dtype_and_na() -> None:
    assert H._parse_path(" a.b . * .c ") == ("a", "b", "*", "c")
    assert H._pd_dtype(DataType.STRING) == "string"
    assert H._pd_dtype(DataType.INTEGER) == "Int64"
    assert H._pd_dtype(DataType.FLOAT) == "float64"
    assert H._pd_dtype(DataType.BOOLEAN) == "boolean"
    assert np.isnan(H._na_for_target(DataType.FLOAT))
    assert H._na_for_target(DataType.STRING) is pd.NA


@pytest.mark.parametrize(
    "value, target, expected",
    [
        (None, DataType.STRING, None),
        ("null", DataType.INTEGER, None),
        (" 1 ", DataType.INTEGER, 1),
        (2.0, DataType.INTEGER, 2),
        (np.int64(7), DataType.INTEGER, 7),
        ("1.0", DataType.INTEGER, 1),
        (3, DataType.FLOAT, 3.0),
        ("1.25", DataType.FLOAT, 1.25),
        (True, DataType.FLOAT, 1.0),
        ("true", DataType.BOOLEAN, True),
        ("False", DataType.BOOLEAN, False),
        (1, DataType.BOOLEAN, True),
        (0.0, DataType.BOOLEAN, False),
        ("abc", DataType.STRING, "abc"),
    ],
)
def test__convert_scalar_ok(value: Any, target: DataType, expected: Any) -> None:
    assert H._convert_scalar(value, target) == expected


def test__convert_scalar_string_preserves_pd_na() -> None:
    out = H._convert_scalar(pd.NA, DataType.STRING)
    assert pd.isna(out)


@pytest.mark.parametrize(
    "value, target, msg",
    [
        (True, DataType.INTEGER, "cannot cast bool to integer"),
        (1.5, DataType.INTEGER, "non-integer float"),
        ("x", DataType.INTEGER, "cannot coerce"),
        (2, DataType.BOOLEAN, ""),
    ],
)
def test__convert_scalar_errors_and_messages(
    value: Any, target: DataType, msg: str
) -> None:
    if target == DataType.BOOLEAN and isinstance(value, int):
        with pytest.raises(ValueError):
            _ = H._convert_scalar(value, DataType.BOOLEAN)
        return
    with pytest.raises(ValueError) as ei:
        _ = H._convert_scalar(value, target)
    assert msg in str(ei.value)


def test__apply_on_error_row_policies() -> None:
    keep, out = H._apply_on_error_row("x", DataType.INTEGER, H.OnError.NULL)
    assert keep and out is None
    keep, out = H._apply_on_error_row("x", DataType.INTEGER, H.OnError.SKIP)
    assert keep and out == "x"
    with pytest.raises(ValueError):
        _ = H._apply_on_error_row("x", DataType.INTEGER, H.OnError.RAISE)


def test__walk_and_convert_nested_list_star_and_missing_keys() -> None:
    row: Dict[str, Any] = {
        "payload": {
            "items": [{"price": "1.5"}, {"price": "2"}, {"price": None}],
            "untouched": [{"x": "no"}],
        },
        "other": 1,
    }
    rules = [
        H.TypeConversionRule("payload.items.*.price", DataType.FLOAT, H.OnError.RAISE),
        H.TypeConversionRule("payload.missing.*.foo", DataType.INTEGER, H.OnError.SKIP),
        H.TypeConversionRule("not_a_dict.foo", DataType.INTEGER, H.OnError.SKIP),
    ]
    keep, out = H.convert_row_nested(row, rules)
    assert keep
    prices = [it["price"] for it in out["payload"]["items"]]
    assert prices == [1.5, 2.0, None]
    assert out["payload"]["untouched"] == [{"x": "no"}]
    assert out["other"] == 1


def test__safe_bool_helpers() -> None:
    assert H._safe_bool("true") is True
    assert H._safe_bool("nope") is None
    assert H._safe_bool_na("nope") is pd.NA


def test_convert_frame_top_level_early_returns() -> None:
    df = pd.DataFrame()
    assert H.convert_frame_top_level(df, []) is df
    assert H.convert_frame_top_level(df, None) is df  # type: ignore[arg-type]


def test_convert_frame_top_level_string_fast_astype_success() -> None:
    df = pd.DataFrame({"s": [1, "x", None]})
    rules = [H.TypeConversionRule("s", DataType.STRING, H.OnError.RAISE)]
    out = H.convert_frame_top_level(df, rules)
    assert list(out["s"].astype("string")) == ["1", "x", pd.NA]


def test_convert_frame_top_level_integer_skip_and_dtype_try() -> None:
    df = pd.DataFrame({"x": ["1", "bad", "2"]})
    rules = [H.TypeConversionRule("x", DataType.INTEGER, H.OnError.SKIP)]
    out = H.convert_frame_top_level(df, rules)
    assert list(out["x"]) == [1, "bad", 2]


def test_convert_frame_top_level_integer_with_bool_fastpath_no_raise() -> None:
    df = pd.DataFrame({"num": [1, True, 3]})
    rules = [H.TypeConversionRule("num", DataType.INTEGER, H.OnError.RAISE)]
    out = H.convert_frame_top_level(df, rules)
    assert list(out["num"].astype("Int64")) == [1, 1, 3]


def test_convert_frame_top_level_integer_raise_on_bool_when_fastpath_disabled() -> None:
    df = pd.DataFrame({"num": [1, True, 3]})
    rules = [H.TypeConversionRule("num", DataType.INTEGER, H.OnError.RAISE)]
    original = H._PD_DTYPES.pop(DataType.INTEGER, None)
    try:
        with pytest.raises(ValueError) as ei:
            _ = H.convert_frame_top_level(df, rules)
        assert "boolean values not allowed for numeric column 'num'" in str(ei.value)
    finally:
        if original is not None:
            H._PD_DTYPES[DataType.INTEGER] = original


def test_convert_frame_top_level_integer_null_for_non_integral() -> None:
    df = pd.DataFrame({"num": ["2", "2.5", "x"]})
    rules = [H.TypeConversionRule("num", DataType.INTEGER, H.OnError.NULL)]
    out = H.convert_frame_top_level(df, rules)
    got = list(out["num"].astype("Int64"))
    assert got == [2, pd.NA, pd.NA]


def test_convert_frame_top_level_float_modes() -> None:
    df = pd.DataFrame({"f": ["1.0", "x", None]})
    with pytest.raises(ValueError):
        _ = H.convert_frame_top_level(df, [H.TypeConversionRule("f", DataType.FLOAT)])
    out_n = H.convert_frame_top_level(
        df, [H.TypeConversionRule("f", DataType.FLOAT, H.OnError.NULL)]
    )
    assert np.isnan(out_n.loc[1, "f"]) and np.isclose(out_n.loc[0, "f"], 1.0)
    out_s = H.convert_frame_top_level(
        df, [H.TypeConversionRule("f", DataType.FLOAT, H.OnError.SKIP)]
    )
    assert out_s.loc[1, "f"] == "x" and np.isclose(float(out_s.loc[0, "f"]), 1.0)


def test_convert_frame_top_level_boolean_all_modes() -> None:
    df = pd.DataFrame({"b": ["true", "0", "weird"]})
    with pytest.raises(ValueError):
        _ = H.convert_frame_top_level(df, [H.TypeConversionRule("b", DataType.BOOLEAN)])
    out_n = H.convert_frame_top_level(
        df, [H.TypeConversionRule("b", DataType.BOOLEAN, H.OnError.NULL)]
    )
    assert list(out_n["b"].astype("boolean")) == [True, False, pd.NA]
    out_s = H.convert_frame_top_level(
        df, [H.TypeConversionRule("b", DataType.BOOLEAN, H.OnError.SKIP)]
    )
    assert list(out_s["b"]) == [True, False, "weird"]


def test_convert_frame_top_level_missing_column_and_multipart_path_are_ignored() -> (
    None
):
    df = pd.DataFrame({"x": ["1"]})
    rules = [
        H.TypeConversionRule("missing", DataType.INTEGER, H.OnError.RAISE),
        H.TypeConversionRule("a.b", DataType.INTEGER, H.OnError.RAISE),
    ]
    out = H.convert_frame_top_level(df, rules)
    assert out.equals(df)


def test_convert_frame_top_level_generic_block_by_disabling_dtype() -> None:
    df = pd.DataFrame({"x": [1, pd.NA, 3]})
    rule = H.TypeConversionRule("x", DataType.STRING, H.OnError.SKIP)
    original = H._PD_DTYPES.pop(DataType.STRING, None)
    try:
        out = H.convert_frame_top_level(df, [rule])
        got = out["x"]

        assert got.iloc[0] == "1"
        assert pd.isna(got.iloc[1])
        assert got.iloc[2] == "3"
    finally:
        if original is not None:
            H._PD_DTYPES[DataType.STRING] = original


def test_convert_dask_top_level_and_meta_fallback() -> None:
    pdf = pd.DataFrame({"x": ["1", "2", "bad"]})
    ddf = dd.from_pandas(pdf, npartitions=2)
    rule = H.TypeConversionRule("x", DataType.INTEGER, H.OnError.SKIP)
    out_ddf = H.convert_dask_top_level(ddf, [rule])
    got = out_ddf.compute()
    assert list(got["x"]) == [1, 2, "bad"]

    def boom(_df: pd.DataFrame, _rules: List[H.TypeConversionRule]) -> pd.DataFrame:
        if len(_df) == 1 and 0 in _df.index:
            raise TypeError("boom")
        return _df

    orig = H.convert_frame_top_level
    try:
        H.convert_frame_top_level = boom  # type: ignore[assignment]
        out_ddf2 = H.convert_dask_top_level(ddf, [rule])
        _ = out_ddf2.compute()
    finally:
        H.convert_frame_top_level = orig  # type: ignore[assignment]


def test_derive_out_schema_for_nested_and_array_and_nullability() -> None:
    in_schema = Schema(
        fields=[
            FieldDef(name="payload", data_type=DataType.OBJECT, children=[]),
            FieldDef(name="top", data_type=DataType.STRING, nullable=False),
        ]
    )
    rules = [
        H.TypeConversionRule("payload.items.*.price", DataType.FLOAT, H.OnError.RAISE),
        H.TypeConversionRule("top", DataType.INTEGER, H.OnError.NULL),
        H.TypeConversionRule("newroot.sub.leaf", DataType.BOOLEAN, H.OnError.RAISE),
    ]
    out_schema = H.derive_out_schema(in_schema, rules)

    def get_field(root: FieldDef, path: str) -> FieldDef:
        parts = path.split(".")
        node = root
        for p in parts[1:]:
            if p == "*":
                node = node.item  # type: ignore[assignment]
                continue
            node = next(c for c in (node.children or []) if c.name == p)
        return node

    fields = {f.name: f for f in out_schema.fields}
    assert "payload" in fields and "newroot" in fields
    price = get_field(fields["payload"], "payload.items.*.price")
    assert price.data_type == DataType.FLOAT
    top = fields["top"]
    assert top.data_type == DataType.INTEGER and top.nullable is True
    leaf = get_field(fields["newroot"], "newroot.sub.leaf")
    assert leaf.data_type == DataType.BOOLEAN


def test_validate_frame_against_schema_success() -> None:
    df = pd.DataFrame(
        {
            "s": pd.Series(["x", "y"], dtype="string"),
            "i": pd.Series([1, 2], dtype="Int64"),
            "f": pd.Series([1.0, 2.5], dtype="float64"),
            "b": pd.Series([True, "false"], dtype="object"),
        }
    )
    schema = Schema(
        fields=[
            FieldDef(name="s", data_type=DataType.STRING, nullable=True),
            FieldDef(name="i", data_type=DataType.INTEGER, nullable=False),
            FieldDef(name="f", data_type=DataType.FLOAT, nullable=False),
            FieldDef(name="b", data_type=DataType.BOOLEAN, nullable=True),
        ]
    )
    H.validate_frame_against_schema(df, schema)


def test_validate_frame_against_schema_missing_and_type_errors() -> None:
    df = pd.DataFrame({"x": [1]})
    schema = Schema(
        fields=[FieldDef(name="y", data_type=DataType.STRING, nullable=True)]
    )
    with pytest.raises(H.SchemaValidationError) as ei1:
        H.validate_frame_against_schema(df, schema)
    assert "missing column 'y'" in str(ei1.value)

    df2 = pd.DataFrame({"i": ["not-int"]})
    schema2 = Schema(
        fields=[FieldDef(name="i", data_type=DataType.INTEGER, nullable=False)]
    )
    with pytest.raises(H.SchemaValidationError) as ei2:
        H.validate_frame_against_schema(df2, schema2)
    assert "expected integer" in str(ei2.value)

    df3 = pd.DataFrame({"b": ["maybe"]})
    schema3 = Schema(
        fields=[FieldDef(name="b", data_type=DataType.BOOLEAN, nullable=False)]
    )
    with pytest.raises(H.SchemaValidationError) as ei3:
        H.validate_frame_against_schema(df3, schema3)
    assert "expected boolean" in str(ei3.value)
