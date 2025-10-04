import pandas as pd
import pytest

from etl_core.utils.common_helpers import (
    get_component_by_name,
    normalize_df,
    assert_unique,
    required_names,
    child_map,
    leaf_field_paths_with_defs,
    leaf_field_paths,
    get_leaf_field_map,
    type_ok_scalar,
    enum_ok,
    ensure_df_columns,
    pandas_flatten_docs,
    unflatten_record,
    unflatten_many,
)
from etl_core.components.wiring.column_definition import FieldDef, DataType
from etl_core.components.wiring.schema import Schema


class DummyComp:
    def __init__(self, name):
        self.name = name


class DummyJob:
    def __init__(self, comps):
        self.components = comps


def test_get_component_by_name_found():
    job = DummyJob([DummyComp("a"), DummyComp("b")])
    c = get_component_by_name(job, "b")
    assert isinstance(c, DummyComp)
    assert c.name == "b"


def test_get_component_by_name_not_found():
    job = DummyJob([DummyComp("a")])
    with pytest.raises(ValueError):
        get_component_by_name(job, "x")


def test_normalize_df_sort_and_reset_index():
    df = pd.DataFrame({"x": [2, 1], "y": ["b", "a"]})
    out = normalize_df(df)  # default: sort by all columns
    assert list(out.index) == [0, 1]
    assert out.to_dict(orient="list") == {"x": [1, 2], "y": ["a", "b"]}

    df2 = pd.DataFrame({"a": [3, 1, 2], "b": ["z", "y", "y"]})
    out2 = normalize_df(df2, sort_cols=["b", "missing"])  # missing is ignored
    assert out2.equals(
        df2.sort_values(by=["b"], kind="mergesort").reset_index(drop=True)
    )


def test_assert_unique_basic_and_key():
    assert_unique([1, 2, 3])  # no error
    with pytest.raises(ValueError):
        assert_unique([1, 2, 1])

    items = [{"id": 1}, {"id": 2}, {"id": 1}]
    with pytest.raises(ValueError):
        assert_unique(items, key=lambda d: d["id"], context="ids")


def build_simple_schema():
    schema = Schema(
        fields=[
            FieldDef(
                name="a",
                data_type=DataType.OBJECT,
                children=[
                    FieldDef(name="b", data_type=DataType.INTEGER, nullable=False),
                    FieldDef(
                        name="c",
                        data_type=DataType.OBJECT,
                        children=[
                            FieldDef(name="d", data_type=DataType.STRING, nullable=True)
                        ],
                    ),
                ],
            ),
            FieldDef(
                name="e",
                data_type=DataType.ARRAY,
                item=FieldDef(name="_", data_type=DataType.INTEGER),
            ),
            FieldDef(
                name="f",
                data_type=DataType.ENUM,
                enum_values=["x", "y"],
                nullable=False,
            ),
            FieldDef(name="p", data_type=DataType.PATH),
        ]
    )
    return schema


def test_required_names_and_child_map():
    children = [
        FieldDef(name="id", data_type=DataType.INTEGER, nullable=False),
        FieldDef(name="opt", data_type=DataType.STRING, nullable=True),
    ]
    assert required_names(children) == {"id"}
    m = child_map(children)
    assert set(m.keys()) == {"id", "opt"}
    assert m["id"].data_type == DataType.INTEGER


def test_leaf_field_paths_and_map():
    schema = build_simple_schema()
    leaves_with_defs = leaf_field_paths_with_defs(schema.fields, sep=".")
    leaves = [p for p, _ in leaves_with_defs]
    assert set(leaves) == {"a.b", "a.c.d", "e", "f", "p"}

    # Check mapping
    mp = get_leaf_field_map(schema, ".")
    assert set(mp.keys()) == set(leaves)
    assert mp["a.b"].data_type == DataType.INTEGER
    assert mp["e"].data_type == DataType.ARRAY

    # Convenience wrapper
    assert set(leaf_field_paths(schema, ".")) == set(leaves)


def test_type_ok_scalar_and_enum_ok():
    fd_str = FieldDef(name="s", data_type=DataType.STRING)
    assert type_ok_scalar("a", fd_str) is True
    assert type_ok_scalar(1, fd_str) is False

    fd_path = FieldDef(name="p", data_type=DataType.PATH)
    assert type_ok_scalar("/tmp", fd_path) is True

    fd_int = FieldDef(name="i", data_type=DataType.INTEGER)
    assert type_ok_scalar(3, fd_int) is True
    assert type_ok_scalar(False, fd_int) is False  # bool is subclass of int -> reject

    fd_float = FieldDef(name="f", data_type=DataType.FLOAT)
    assert type_ok_scalar(2.5, fd_float) is True
    assert type_ok_scalar(2, fd_float) is True  # ints ok for float
    assert type_ok_scalar(True, fd_float) is False

    fd_bool = FieldDef(name="b", data_type=DataType.BOOLEAN)
    assert type_ok_scalar(True, fd_bool) is True
    assert type_ok_scalar(1, fd_bool) is False

    fd_enum = FieldDef(
        name="e", data_type=DataType.ENUM, enum_values=["x", "y"], nullable=True
    )
    assert type_ok_scalar("x", fd_enum) is True
    assert type_ok_scalar(1, fd_enum) is True
    assert enum_ok("x", fd_enum) is True
    assert enum_ok("z", fd_enum) is False

    fd_enum_empty = FieldDef(
        name="e2", data_type=DataType.ENUM, enum_values=[], nullable=True
    )
    assert enum_ok("anything", fd_enum_empty) is False
    # Nulls allowed for enum
    assert enum_ok(None, fd_enum) is True
    assert type_ok_scalar(None, fd_enum) is True


def test_ensure_df_columns_success_and_errors():
    schema = build_simple_schema()
    cols_ok = ["a.b", "a.c.d", "e", "f", "p"]
    ensure_df_columns(cols_ok, schema, schema_name="S", sep=".")

    with pytest.raises(ValueError) as ei:
        ensure_df_columns(["a.b", "e", "f", "p"], schema, schema_name="S", sep=".")
    assert "missing required columns" in str(ei.value)

    with pytest.raises(ValueError) as ei2:
        ensure_df_columns(cols_ok + ["extra"], schema, schema_name="S", sep=".")
    assert "unknown columns present" in str(ei2.value)


def test_pandas_flatten_and_unflatten():
    docs = [{"a": {"b": 1}, "e": [1, 2], "f": "x", "p": "path"}]
    df = pandas_flatten_docs(docs, sep=".")
    assert list(df.columns) == ["a.b", "e", "f", "p"] or set(df.columns) == {
        "a.b",
        "e",
        "f",
        "p",
    }

    # empty input
    df_empty = pandas_flatten_docs([], sep=".")
    assert df_empty.empty

    # unflatten
    flat = {"a.b": 1, "a.c.d": 2, "e": [1, 2]}
    nested = unflatten_record(flat, sep=".")
    assert nested == {"a": {"b": 1, "c": {"d": 2}}, "e": [1, 2]}

    many = unflatten_many([flat, {"f": "x"}], sep=".")
    assert many == [{"a": {"b": 1, "c": {"d": 2}}, "e": [1, 2]}, {"f": "x"}]
