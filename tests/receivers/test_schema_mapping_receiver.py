from __future__ import annotations

from typing import Any, Dict, List, Tuple

import dask.dataframe as dd
import pandas as pd
from pandas.testing import assert_frame_equal

from etl_core.receivers.data_operations_receivers.schema_mapping.schema_mapping_receiver import (  # noqa: E501
    SchemaMappingReceiver,
    SchemaPath,
    _read_path,
    _write_path,
    _map_dataframe,
    _infer_meta_from_pairs,
)
from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef


def _schema_from_fields(fields: List[FieldDef]) -> Schema:
    return Schema(fields=fields)


def _fd(
    name: str,
    data_type: str,
    children: List[FieldDef] | None = None,
) -> FieldDef:
    return FieldDef(name=name, data_type=data_type, children=children or [])


def _flat_schema(*cols: str) -> Schema:
    return _schema_from_fields([_fd(c, "string") for c in cols])


def _nested_user_schema() -> Schema:
    # Nested user object with id/name/address.city for row-path tests
    return _schema_from_fields(
        [
            _fd(
                "user",
                "object",
                children=[
                    _fd("id", "integer"),
                    _fd("name", "string"),
                    _fd("address", "object", children=[_fd("city", "string")]),
                ],
            )
        ]
    )


def _map_rules(
    *items: Tuple[str, str, str, str],
) -> List[Tuple[str, str, str, str]]:
    return [tuple(x) for x in items]  # type: ignore[return-value]


def test_receiver_row_nested_in_to_flat_out() -> None:
    """Nested input → flat outputs on two ports (fan-out)."""
    recv = SchemaMappingReceiver()
    rules = _map_rules(
        ("in", "user.id", "flatA", "uid"),
        ("in", "user.address.city", "flatB", "city"),
    )

    row: Dict[str, Any] = {
        "user": {
            "id": 7,
            "name": "Nina",
            "address": {"city": "Berlin", "zip": "10115"},
        }
    }

    outs = list(recv.map_row(row=row, rules=rules))
    by_port: Dict[str, Dict[str, Any]] = {}
    for p, payload in outs:
        by_port[p] = payload

    assert by_port["flatA"] == {"uid": 7}
    assert by_port["flatB"] == {"city": "Berlin"}


def test_receiver_row_flat_in_to_nested_out() -> None:
    """Flat input → nested output on a single port."""
    recv = SchemaMappingReceiver()
    rules = _map_rules(
        ("in", "id", "dst", "user.id"),
        ("in", "name", "dst", "user.profile.name"),
    )

    row = {"id": 1, "name": "Max"}
    outs = list(recv.map_row(row=row, rules=rules))
    assert len(outs) == 1 and outs[0][0] == "dst"
    assert outs[0][1] == {"user": {"id": 1, "profile": {"name": "Max"}}}


def test_receiver_row_multiple_in_to_one_out() -> None:
    """Rules reference different logical src ports; merged into one dest."""
    recv = SchemaMappingReceiver()
    rules = _map_rules(("A", "id", "out", "uid"), ("B", "name", "out", "uname"))

    row = {"id": 11, "name": "Luca"}
    outs = list(recv.map_row(row=row, rules=rules))
    assert len(outs) == 1 and outs[0][0] == "out"
    assert outs[0][1] == {"uid": 11, "uname": "Luca"}


def test_receiver_bulk_nested_in_to_flat_out() -> None:
    """Nested-in expressed by flattened columns → flat out."""
    recv = SchemaMappingReceiver()
    rules = _map_rules(
        ("in", "user.id", "dst", "uid"),
        ("in", "user.address.city", "dst", "city"),
    )

    df = pd.DataFrame(
        [
            {"user.id": 1, "user.address.city": "Berlin"},
            {"user.id": 2, "user.address.city": "Hamburg"},
        ]
    )

    outs = list(
        recv.map_bulk(
            dataframe=df,
            rules=rules,
            out_port_schemas={"dst": _flat_schema("uid", "city")},
            path_separator=".",
        )
    )
    assert len(outs) == 1 and outs[0][0] == "dst"
    got = outs[0][1].reset_index(drop=True)
    expected = pd.DataFrame({"uid": [1, 2], "city": ["Berlin", "Hamburg"]})
    assert_frame_equal(got, expected, check_dtype=False)


def test_receiver_bulk_flat_in_to_nested_out() -> None:
    """Flat input → nested destination names (column dots)."""
    recv = SchemaMappingReceiver()
    rules = _map_rules(
        ("in", "id", "dst", "user.id"),
        ("in", "name", "dst", "user.profile.name"),
    )

    df = pd.DataFrame([{"id": 1, "name": "A"}, {"id": 2, "name": "B"}])
    outs = list(
        recv.map_bulk(
            dataframe=df,
            rules=rules,
            out_port_schemas={"dst": _flat_schema("user.id", "user.profile.name")},
            path_separator=".",
        )
    )
    assert len(outs) == 1 and outs[0][0] == "dst"

    got = outs[0][1].reset_index(drop=True)
    expected = pd.DataFrame({"user.id": [1, 2], "user.profile.name": ["A", "B"]})
    assert_frame_equal(got, expected, check_dtype=False)


def test_receiver_bulk_multiple_in_to_one_out() -> None:
    """Mix src_port labels; all fields land in one destination port."""
    recv = SchemaMappingReceiver()
    rules = _map_rules(("A", "id", "both", "uid"), ("B", "name", "both", "uname"))

    df = pd.DataFrame([{"id": 9, "name": "X"}, {"id": 10, "name": "Y"}])
    outs = list(
        recv.map_bulk(
            dataframe=df,
            rules=rules,
            out_port_schemas={"both": _flat_schema("uid", "uname")},
            path_separator=".",
        )
    )
    assert len(outs) == 1 and outs[0][0] == "both"
    got = outs[0][1].reset_index(drop=True)
    expected = pd.DataFrame({"uid": [9, 10], "uname": ["X", "Y"]})
    assert_frame_equal(got, expected, check_dtype=False)


def test_receiver_bigdata_flat_in_to_nested_out() -> None:
    """Dask DataFrame mapping with dotted destination names."""
    recv = SchemaMappingReceiver()
    rules = _map_rules(
        ("in", "id", "dst", "user.id"),
        ("in", "name", "dst", "user.name"),
    )

    pdf = pd.DataFrame([{"id": 1, "name": "A"}, {"id": 2, "name": "B"}])
    ddf = dd.from_pandas(pdf, npartitions=2)

    outs_bd = list(
        recv.map_bigdata(
            ddf=ddf,
            rules=rules,
            out_port_schemas={"dst": _flat_schema("user.id", "user.name")},
            path_separator=".",
        )
    )
    port, out_ddf = outs_bd[0]
    assert port == "dst"

    got = out_ddf.compute().sort_values(["user.id"]).reset_index(drop=True)
    expected = pd.DataFrame({"user.id": [1, 2], "user.name": ["A", "B"]})
    assert_frame_equal(got, expected, check_dtype=False)


def test_path_helpers_read_write_nested() -> None:
    src = {"a": {"b": {"c": 42}}}
    assert _read_path(src, SchemaPath.parse("a.b.c")) == 42
    assert _read_path(src, SchemaPath.parse("a.b.nope")) is None

    dst: Dict[str, Any] = {}
    _write_path(dst, SchemaPath.parse("x.y.z"), "val")
    assert dst == {"x": {"y": {"z": "val"}}}


def test_map_dataframe_and_infer_meta() -> None:
    df = pd.DataFrame({"id": [1, 2], "name": ["N", "M"]})
    pairs = [("id", "uid"), ("name", "uname"), ("missing", "m")]

    mapped = _map_dataframe(df, pairs)
    expected = pd.DataFrame({"uid": [1, 2], "uname": ["N", "M"], "m": [None, None]})
    assert_frame_equal(mapped, expected, check_dtype=False)

    ddf = dd.from_pandas(df, npartitions=1)
    meta = _infer_meta_from_pairs(ddf, pairs)
    assert set(meta.columns) == {"uid", "uname", "m"}
