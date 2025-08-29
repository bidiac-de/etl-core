import pytest
import pandas as pd
import dask.dataframe as dd

from etl_core.components.wiring.validation import (
    validate_row_against_schema,
    validate_dataframe_against_schema,
    validate_dask_dataframe_against_schema,
)
from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef, DataType


def _schema() -> Schema:
    # Nested object, array leaf, enum leaf, and a PATH leaf
    return Schema(
        fields=[
            FieldDef(name="id", data_type=DataType.INTEGER, nullable=False),
            FieldDef(name="name", data_type=DataType.STRING, nullable=False),
            FieldDef(
                name="address",
                data_type=DataType.OBJECT,
                nullable=False,
                children=[
                    FieldDef(name="street", data_type=DataType.STRING, nullable=False),
                    FieldDef(name="zip", data_type=DataType.STRING, nullable=True),
                ],
            ),
            FieldDef(
                name="tags",
                data_type=DataType.ARRAY,
                nullable=True,
                item=FieldDef(name="tag", data_type=DataType.STRING, nullable=True),
            ),
            FieldDef(
                name="status",
                data_type=DataType.ENUM,
                nullable=False,
                enum_values=["active", "blocked"],
            ),
            FieldDef(name="pathcol", data_type=DataType.PATH, nullable=True),
        ]
    )


def _good_row() -> dict:
    return {
        "id": 1,
        "name": "Nina",
        "address": {"street": "Main", "zip": None},
        "tags": ["a", "b"],
        "status": "active",
        "pathcol": "/tmp/file.txt",
    }


def _good_df(sep: str = ".") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 2],
            "name": ["Nina", "Max"],
            f"address{sep}street": ["Main", "Elm"],
            f"address{sep}zip": [None, "90210"],
            "tags": [["a"], ["b"]],
            "status": ["active", "blocked"],
            "pathcol": ["/x", None],
        }
    )


# -----------------------
# Row validation — OK
# -----------------------


def test_row_ok() -> None:
    validate_row_against_schema(_good_row(), _schema(), schema_name="row-ok")


def test_row_ok_with_custom_separator() -> None:
    # Row validation doesn’t use the separator for traversal, but pass it to cover API
    validate_row_against_schema(
        _good_row(), _schema(), schema_name="row-ok-colon", path_separator=":"
    )


# -----------------------
# Row validation — failures
# -----------------------


@pytest.mark.parametrize(
    "bad_row, expected_fragment",
    [
        ({**_good_row(), "id": "x"}, "expected DataType.INTEGER"),
        ({**_good_row(), "status": "paused"}, "must be one of"),
        (
            {k: v for k, v in _good_row().items() if k != "name"},
            "missing required fields",
        ),
        ({**_good_row(), "unknown": 1}, "unknown fields present"),
    ],
)
def test_row_top_level_failures(bad_row: dict, expected_fragment: str) -> None:
    with pytest.raises(ValueError) as ex:
        validate_row_against_schema(bad_row, _schema(), schema_name="row-bad")
    assert expected_fragment in str(ex.value)


def test_row_nested_missing_and_unknown() -> None:
    s = _schema()
    bad = _good_row()
    bad["address"] = {"zip": "x", "whoops": 3}
    with pytest.raises(ValueError) as ex:
        validate_row_against_schema(bad, s, schema_name="row-nested-bad")
    msg = str(ex.value)
    assert "missing" in msg or "unknown" in msg


def test_row_array_item_type_mismatch() -> None:
    s = _schema()
    bad = _good_row()
    bad["tags"] = [1, 2, 3]
    with pytest.raises(ValueError) as ex:
        validate_row_against_schema(bad, s, schema_name="row-array-bad")
    assert "expected DataType.STRING" in str(ex.value)


def test_row_array_structure_only_when_item_none() -> None:
    s = Schema(
        fields=[
            FieldDef(name="data", data_type=DataType.ARRAY, nullable=False, item=None)
        ]
    )
    ok = {"data": [1, "x", {"y": 2}]}
    validate_row_against_schema(ok, s, schema_name="row-array-struct-ok")

    bad = {"data": "not-a-list"}
    with pytest.raises(ValueError):
        validate_row_against_schema(bad, s, schema_name="row-array-struct-bad")


# -----------------------
# Pandas DataFrame validation
# -----------------------


def test_dataframe_ok() -> None:
    validate_dataframe_against_schema(_good_df(), _schema(), schema_name="df-ok")


def test_dataframe_ok_with_custom_separator() -> None:
    validate_dataframe_against_schema(
        _good_df(sep=":"), _schema(), schema_name="df-ok-colon", path_separator=":"
    )


def test_dataframe_missing_and_unknown_columns() -> None:
    df_missing = _good_df().drop(columns=["name"])
    with pytest.raises(ValueError) as ex1:
        validate_dataframe_against_schema(
            df_missing, _schema(), schema_name="df-missing"
        )
    assert "missing required columns" in str(ex1.value)

    df_extra = _good_df().assign(extra=[1, 2])
    with pytest.raises(ValueError) as ex2:
        validate_dataframe_against_schema(df_extra, _schema(), schema_name="df-unknown")
    assert "unknown columns present" in str(ex2.value)


def test_dataframe_enum_and_null_failures() -> None:
    df_enum = _good_df()
    df_enum.loc[0, "status"] = "weird"
    with pytest.raises(ValueError) as ex1:
        validate_dataframe_against_schema(df_enum, _schema(), schema_name="df-enum-bad")
    assert "outside ['active', 'blocked']" in str(ex1.value)

    df_null = _good_df()
    df_null.loc[1, "id"] = None
    with pytest.raises(ValueError) as ex2:
        validate_dataframe_against_schema(df_null, _schema(), schema_name="df-null-bad")
    assert "contains nulls" in str(ex2.value)


# -----------------------
# Dask DataFrame validation
# -----------------------


def test_dask_dataframe_ok() -> None:
    ddf = dd.from_pandas(_good_df(), npartitions=2)
    validate_dask_dataframe_against_schema(ddf, _schema(), schema_name="ddf-ok")


def test_dask_missing_unknown_enum_and_null_failures() -> None:
    pdf = _good_df()

    # missing
    ddf_missing = dd.from_pandas(pdf.drop(columns=["name"]), npartitions=1)
    with pytest.raises(ValueError) as ex1:
        validate_dask_dataframe_against_schema(
            ddf_missing, _schema(), schema_name="ddf-missing"
        )
    assert "missing required columns" in str(ex1.value)

    # unknown
    ddf_extra = dd.from_pandas(pdf.assign(extra=[0, 1]), npartitions=1)
    with pytest.raises(ValueError) as ex2:
        validate_dack_dataframe_against_schema = (
            validate_dask_dataframe_against_schema  # alias for flake8 naming
        )
        validate_dack_dataframe_against_schema(
            ddf_extra, _schema(), schema_name="ddf-unknown"
        )
    assert "unknown columns present" in str(ex2.value)

    # enum
    pdf_enum = pdf.copy()
    pdf_enum.loc[0, "status"] = "nope"
    ddf_enum = dd.from_pandas(pdf_enum, npartitions=1)
    with pytest.raises(ValueError) as ex3:
        validate_dask_dataframe_against_schema(
            ddf_enum, _schema(), schema_name="ddf-enum-bad"
        )
    assert "outside ['active', 'blocked']" in str(ex3.value)

    # null
    pdf_null = pdf.copy()
    pdf_null.loc[0, "id"] = None
    ddf_null = dd.from_pandas(pdf_null, npartitions=1)
    with pytest.raises(ValueError) as ex4:
        validate_dask_dataframe_against_schema(
            ddf_null, _schema(), schema_name="ddf-null-bad"
        )
    assert "contains nulls" in str(ex4.value)
