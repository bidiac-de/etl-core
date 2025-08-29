from __future__ import annotations

from typing import Any, Dict, List, Tuple, TypeVar

import dask.dataframe as dd
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa: E501
    DataOperationsMetrics,
)
from etl_core.receivers.data_operations_receivers.schema_mapping.schema_mapping_receiver import (  # noqa: E501
    SchemaMappingReceiver,
    schema_path,
    _read_path,
    _write_path,
    _map_dataframe,
    _infer_meta_from_pairs,
)
from tests.helpers import collect_async
from tests.components.data_operations.data_ops_helpers import mapping_rules_iter

T = TypeVar("T")


@pytest.mark.asyncio
async def test_receiver_row_nested_in_to_flat_out(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    """Nested input → flat outputs on two ports (fan-out)."""
    recv = SchemaMappingReceiver()

    row: Dict[str, Any] = {
        "user": {
            "id": 7,
            "name": "Nina",
            "address": {"city": "Berlin", "zip": "10115"},
        }
    }

    rules = mapping_rules_iter(
        ("in", "user.id", "flatA", "uid"),
        ("in", "user.address.city", "flatB", "city"),
    )

    outs: List[Tuple[str, Dict[str, Any]]] = await collect_async(
        recv.process_row(row=row, metrics=data_ops_metrics, rules=rules)
    )
    by_port: Dict[str, Dict[str, Any]] = {}
    for p, payload in outs:
        by_port[p] = payload

    assert by_port["flatA"] == {"uid": 7}
    assert by_port["flatB"] == {"city": "Berlin"}
    assert data_ops_metrics.lines_received == 1
    assert data_ops_metrics.lines_processed == 2
    assert data_ops_metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_receiver_row_flat_in_to_nested_out(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    """Flat input → nested output on a single port."""
    recv = SchemaMappingReceiver()

    row = {"id": 1, "name": "Max"}
    rules = mapping_rules_iter(
        ("in", "id", "dst", "user.id"),
        ("in", "name", "dst", "user.profile.name"),
    )

    outs: List[Tuple[str, Dict[str, Any]]] = await collect_async(
        recv.process_row(row=row, metrics=data_ops_metrics, rules=rules)
    )
    assert len(outs) == 1 and outs[0][0] == "dst"
    nested = outs[0][1]
    assert nested == {"user": {"id": 1, "profile": {"name": "Max"}}}
    assert data_ops_metrics.lines_received == 1
    assert data_ops_metrics.lines_processed == 1
    assert data_ops_metrics.lines_forwarded == 1


@pytest.mark.asyncio
async def test_receiver_row_multiple_in_to_one_out(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    """Rules reference multiple src ports logically, merged into one destination."""
    recv = SchemaMappingReceiver()

    row = {"id": 11, "name": "Luca"}
    rules = mapping_rules_iter(
        ("A", "id", "out", "uid"),
        ("B", "name", "out", "uname"),
    )

    outs: List[Tuple[str, Dict[str, Any]]] = await collect_async(
        recv.process_row(row=row, metrics=data_ops_metrics, rules=rules)
    )
    assert len(outs) == 1 and outs[0][0] == "out"
    assert outs[0][1] == {"uid": 11, "uname": "Luca"}
    assert data_ops_metrics.lines_received == 1
    assert data_ops_metrics.lines_processed == 1
    assert data_ops_metrics.lines_forwarded == 1


@pytest.mark.asyncio
async def test_receiver_bulk_nested_in_to_flat_out(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    """nested-in by flattened columns."""
    recv = SchemaMappingReceiver()

    df = pd.DataFrame(
        [
            {"user.id": 1, "user.address.city": "Berlin"},
            {"user.id": 2, "user.address.city": "Hamburg"},
        ]
    )

    rules = mapping_rules_iter(
        ("in", "user.id", "dst", "uid"),
        ("in", "user.address.city", "dst", "city"),
    )

    outs: List[Tuple[str, pd.DataFrame]] = await collect_async(
        recv.process_bulk(dataframe=df, metrics=data_ops_metrics, rules=rules)
    )
    assert len(outs) == 1 and outs[0][0] == "dst"
    got = outs[0][1].reset_index(drop=True)
    expected = pd.DataFrame({"uid": [1, 2], "city": ["Berlin", "Hamburg"]})
    assert_frame_equal(got, expected, check_dtype=False)

    assert data_ops_metrics.lines_received == 2
    assert data_ops_metrics.lines_processed == 2
    assert data_ops_metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_receiver_bulk_flat_in_to_nested_out(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    """Flat input → nested destination names."""
    recv = SchemaMappingReceiver()

    df = pd.DataFrame([{"id": 1, "name": "A"}, {"id": 2, "name": "B"}])
    rules = mapping_rules_iter(
        ("in", "id", "dst", "user.id"),
        ("in", "name", "dst", "user.profile.name"),
    )

    outs: List[Tuple[str, pd.DataFrame]] = await collect_async(
        recv.process_bulk(dataframe=df, metrics=data_ops_metrics, rules=rules)
    )
    assert len(outs) == 1 and outs[0][0] == "dst"

    got = outs[0][1].reset_index(drop=True)
    expected = pd.DataFrame({"user.id": [1, 2], "user.profile.name": ["A", "B"]})
    assert_frame_equal(got, expected, check_dtype=False)

    assert data_ops_metrics.lines_received == 2
    assert data_ops_metrics.lines_processed == 2
    assert data_ops_metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_receiver_bulk_multiple_in_to_one_out(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    """Mix src_port labels; all fields land in one destination port."""
    recv = SchemaMappingReceiver()

    df = pd.DataFrame([{"id": 9, "name": "X"}, {"id": 10, "name": "Y"}])
    rules = mapping_rules_iter(
        ("A", "id", "both", "uid"),
        ("B", "name", "both", "uname"),
    )

    outs: List[Tuple[str, pd.DataFrame]] = await collect_async(
        recv.process_bulk(dataframe=df, metrics=data_ops_metrics, rules=rules)
    )
    assert len(outs) == 1 and outs[0][0] == "both"
    got = outs[0][1].reset_index(drop=True)
    expected = pd.DataFrame({"uid": [9, 10], "uname": ["X", "Y"]})
    assert_frame_equal(got, expected, check_dtype=False)

    assert data_ops_metrics.lines_received == 2
    assert data_ops_metrics.lines_processed == 2
    assert data_ops_metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_receiver_bigdata_flat_in_to_nested_out(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    recv = SchemaMappingReceiver()

    pdf = pd.DataFrame([{"id": 1, "name": "A"}, {"id": 2, "name": "B"}])
    ddf = dd.from_pandas(pdf, npartitions=2)
    rules = mapping_rules_iter(
        ("in", "id", "dst", "user.id"),
        ("in", "name", "dst", "user.name"),
    )

    outs_bd: List[Tuple[str, dd.DataFrame]] = await collect_async(
        recv.process_bigdata(ddf, metrics=data_ops_metrics, rules=rules)
    )
    port, out_ddf = outs_bd[0]
    assert port == "dst"

    got = out_ddf.compute().sort_values(["user.id"]).reset_index(drop=True)
    expected = pd.DataFrame({"user.id": [1, 2], "user.name": ["A", "B"]})
    assert_frame_equal(got, expected, check_dtype=False)

    assert data_ops_metrics.lines_processed == 2
    assert data_ops_metrics.lines_forwarded == 2
    assert data_ops_metrics.lines_received == 2


def test_path_helpers_read_write_nested() -> None:
    src = {"a": {"b": {"c": 42}}}
    assert _read_path(src, schema_path.parse("a.b.c")) == 42
    assert _read_path(src, schema_path.parse("a.b.nope")) is None

    dst: Dict[str, Any] = {}
    _write_path(dst, schema_path.parse("x.y.z"), "val")
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
