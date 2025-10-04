import numpy as np
import pandas as pd
import dask.dataframe as dd
import pytest

from etl_core.receivers.data_operations_receivers.type_conversion.type_conversion_helper import (  # noqa: E501
    OnError,
    TypeConversionRule,
    _convert_scalar,
    _walk_and_convert,
    convert_dask_top_level,
    convert_frame_top_level,
    derive_out_schema,
    validate_frame_against_schema,
    SchemaValidationError,
)
from etl_core.components.wiring.column_definition import DataType, FieldDef
from etl_core.components.wiring.schema import Schema


def test__convert_scalar_integer_returns_none_for_na() -> None:
    assert _convert_scalar(np.nan, DataType.INTEGER) is None
    assert _convert_scalar(pd.NA, DataType.INTEGER) is None


def test__convert_scalar_float_returns_none_for_na() -> None:
    assert _convert_scalar(pd.NA, DataType.FLOAT) is None
    assert _convert_scalar(np.nan, DataType.FLOAT) is None


def test__convert_scalar_boolean_variants() -> None:
    assert _convert_scalar(pd.NA, DataType.BOOLEAN) is None

    assert _convert_scalar(np.bool_(True), DataType.BOOLEAN) is True

    with pytest.raises(ValueError, match="cannot coerce float 0.5 to boolean"):
        _ = _convert_scalar(0.5, DataType.BOOLEAN)


def test__walk_and_convert_obj_is_none() -> None:
    keep, out = _walk_and_convert(
        obj=None,
        parts=("payload",),
        target=DataType.STRING,
        policy=OnError.RAISE,
    )
    assert keep is True
    assert out is None


def test__walk_and_convert_star_on_non_list() -> None:
    obj = {"a": 1}
    keep, out = _walk_and_convert(
        obj=obj,
        parts=("*", "a"),
        target=DataType.INTEGER,
        policy=OnError.RAISE,
    )
    assert keep is True
    assert out == obj  # unchanged


def test__walk_and_convert_non_mapping_non_list() -> None:
    obj = 42
    keep, out = _walk_and_convert(
        obj=obj,
        parts=("a",),
        target=DataType.INTEGER,
        policy=OnError.RAISE,
    )
    assert keep is True
    assert out == 42


def test_convert_frame_top_level_mask_bools_when_not_raise() -> None:
    df = pd.DataFrame({"x": [1, True, 3]})
    rules = [
        TypeConversionRule(
            column_path="x", target=DataType.INTEGER, on_error=OnError.NULL
        )
    ]
    out = convert_frame_top_level(df, rules)
    assert str(out["x"].dtype) == "Int64"
    assert out["x"].isna().sum() == 1
    assert list(out["x"]) == [1, pd.NA, 3]


def test_convert_frame_top_level_integer_raise_no_invalids_assigns_int64() -> None:
    df = pd.DataFrame({"x": [1, 2, "3"]})
    rules = [
        TypeConversionRule(
            column_path="x", target=DataType.INTEGER, on_error=OnError.RAISE
        )
    ]
    out = convert_frame_top_level(df, rules)

    assert list(out["x"].astype("Int64")) == [1, 2, 3]


def test_convert_frame_top_level_float_raise_no_new_nulls_assigns_float64() -> None:
    df = pd.DataFrame({"x": [1.2, "3.4", None]})
    rules = [
        TypeConversionRule(
            column_path="x", target=DataType.FLOAT, on_error=OnError.RAISE
        )
    ]
    out = convert_frame_top_level(df, rules)
    assert str(out["x"].dtype) == "float64"
    assert pytest.approx(out["x"].iloc[0], rel=1e-9) == 1.2
    assert pytest.approx(out["x"].iloc[1], rel=1e-9) == 3.4
    assert pd.isna(out["x"].iloc[2])


def test_convert_dask_top_level_early_return_without_rules() -> None:
    pdf = pd.DataFrame({"a": [1, 2, 3]})
    ddf = dd.from_pandas(pdf, npartitions=1)
    out = convert_dask_top_level(ddf, rules=[])
    assert out.compute().equals(pdf)


def test_convert_dask_top_level_none_input() -> None:
    assert convert_dask_top_level(None, rules=[]) is None  # type: ignore[arg-type]


def test_derive_out_schema_forces_object_on_nested_rule_and_sets_nullable() -> None:
    in_schema = Schema(
        fields=[
            FieldDef(
                name="payload",
                data_type=DataType.INTEGER,
                children=None,
                nullable=False,
            )
        ],
    )
    rules = [
        TypeConversionRule(
            column_path="payload.child",
            target=DataType.STRING,
            on_error=OnError.NULL,
        )
    ]
    out_schema = derive_out_schema(in_schema, rules)

    root = next(f for f in out_schema.fields if f.name == "payload")
    assert root.data_type == DataType.OBJECT
    assert root.children is not None and len(root.children) == 1

    child = root.children[0]
    assert child.name == "child"
    assert child.data_type == DataType.STRING
    assert child.nullable is True


def test__convert_scalar_boolean_na_branch() -> None:
    assert _convert_scalar(pd.NA, DataType.BOOLEAN) is None


def test__walk_and_convert_list_index_error_returns_skip() -> None:
    obj = [{"x": "1"}]
    keep, out = _walk_and_convert(
        obj=obj,
        parts=("0", "x"),
        target=DataType.STRING,
        policy=OnError.SKIP,
    )
    assert keep is True and out == obj


def test__walk_and_convert_star_with_invalid_child_type() -> None:
    obj = [1, 2, 3]
    keep, out = _walk_and_convert(
        obj=obj,
        parts=("*", "x"),
        target=DataType.INTEGER,
        policy=OnError.RAISE,
    )
    assert keep and out == obj


def test_convert_frame_top_level_boolean_raise_null_masking() -> None:
    df = pd.DataFrame({"flag": ["yes", "no", None]})
    rules = [TypeConversionRule("flag", DataType.BOOLEAN, OnError.NULL)]
    out = convert_frame_top_level(df, rules)

    assert out["flag"].isna().sum() == 1
    vals = {str(v).lower() for v in out["flag"].dropna()}
    assert vals <= {"true", "false"}


def test_convert_dask_top_level_meta_failure(monkeypatch) -> None:
    pdf = pd.DataFrame({"a": ["1", "bad"]})
    ddf = dd.from_pandas(pdf, npartitions=1)
    rules = [TypeConversionRule("a", DataType.INTEGER, OnError.RAISE)]

    callcount = {"n": 0}

    def boom_once(df, rules_):
        callcount["n"] += 1
        if callcount["n"] == 1:
            raise TypeError("meta infer fail")
        return df

    monkeypatch.setattr(
        "etl_core.receivers.data_operations_receivers.type_conversion."
        "type_conversion_helper.convert_frame_top_level",
        boom_once,
    )
    out = convert_dask_top_level(ddf, rules)
    result = out.compute()
    assert list(result["a"]) == ["1", "bad"]


def test_derive_out_schema_creates_children_from_none() -> None:
    in_schema = Schema(
        fields=[
            FieldDef(
                name="root",
                data_type=DataType.OBJECT,
                children=None,
            )
        ]
    )
    rules = [TypeConversionRule("root.newfield", DataType.FLOAT, OnError.RAISE)]
    out = derive_out_schema(in_schema, rules)
    root = next(f for f in out.fields if f.name == "root")
    assert any(c.name == "newfield" for c in (root.children or []))


def test_validate_frame_against_schema() -> None:
    df_ok = pd.DataFrame({"maybe": [None, "true", "false"]})
    schema_ok = Schema(
        fields=[FieldDef(name="maybe", data_type=DataType.BOOLEAN, nullable=True)]
    )
    validate_frame_against_schema(df_ok, schema_ok)

    df_bad = pd.DataFrame({"maybe": ["true", "nope"]})
    schema_strict = Schema(
        fields=[FieldDef(name="maybe", data_type=DataType.BOOLEAN, nullable=False)]
    )
    with pytest.raises(SchemaValidationError):
        validate_frame_against_schema(df_bad, schema_strict)
