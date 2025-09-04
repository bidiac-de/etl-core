from __future__ import annotations

from typing import Any, Dict, List

import dask.dataframe as dd
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa: E501
    DataOperationsMetrics,
)
from etl_core.receivers.data_operations_receivers.aggregation.aggregation_receiver import (  # noqa: E501
    AggregationReceiver,
)


@pytest.mark.asyncio
async def test_receiver_rows_groupby_sum_mean_and_count_star(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    rows: List[Dict[str, Any]] = [
        {"cat": "A", "value": 1},
        {"cat": "A", "value": 2},
        {"cat": "B", "value": 10},
    ]
    aggs = [
        {"src": "value", "op": "sum", "dest": "v_sum"},
        {"src": "value", "op": "mean", "dest": "v_mean"},
        {"src": "*", "op": "count", "dest": "n"},
    ]
    recv = AggregationReceiver()

    out_rows: List[Dict[str, Any]] = []
    async for port, payload in recv.process_rows(
        rows=rows, group_by=["cat"], aggregations=aggs, metrics=data_ops_metrics
    ):
        assert port == "out"
        out_rows.append(payload)

    got = pd.DataFrame(out_rows).sort_values("cat").reset_index(drop=True)
    expected = (
        pd.DataFrame(
            [
                {"cat": "A", "v_sum": 3, "v_mean": 1.5, "n": 2},
                {"cat": "B", "v_sum": 10, "v_mean": 10.0, "n": 1},
            ]
        )
        .sort_values("cat")
        .reset_index(drop=True)
    )
    assert_frame_equal(got, expected, check_dtype=False)

    assert data_ops_metrics.lines_received == 3
    assert data_ops_metrics.lines_processed == 2
    assert data_ops_metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_receiver_bulk_no_group_keys_uses_const_key_and_strips(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    df = pd.DataFrame([{"v": 1}, {"v": 2}, {"v": 3}])
    aggs = [
        {"src": "v", "op": "sum", "dest": "total"},
        {"src": "*", "op": "count", "dest": "n"},
    ]
    recv = AggregationReceiver()

    outs: List[pd.DataFrame] = []
    async for port, payload in recv.process_bulk(
        dataframe=df, group_by=[], aggregations=aggs, metrics=data_ops_metrics
    ):
        assert port == "out"
        outs.append(payload)

    assert len(outs) == 1
    out = outs[0]
    assert list(out.columns) == ["total", "n"]
    assert out.shape[0] == 1
    assert out.loc[0, "total"] == 6
    assert out.loc[0, "n"] == 3

    assert data_ops_metrics.lines_received == 3
    assert data_ops_metrics.lines_processed == 1
    assert data_ops_metrics.lines_forwarded == 1


@pytest.mark.asyncio
async def test_receiver_bigdata_multi_aggs_rename_and_order(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    pdf = pd.DataFrame(
        [
            {"k": "x", "a": 1, "name": "Alice"},
            {"k": "x", "a": 3, "name": "Bob"},
            {"k": "y", "a": 2, "name": "Bob"},
            {"k": "y", "a": 8, "name": "Alice"},
        ]
    )
    ddf = dd.from_pandas(pdf, npartitions=2)
    aggs = [
        {"src": "a", "op": "min", "dest": "a_min"},
        {"src": "a", "op": "max", "dest": "a_max"},
        {"src": "name", "op": "nunique", "dest": "names"},
        {"src": "*", "op": "count", "dest": "n"},
    ]

    recv = AggregationReceiver()
    outs: List[dd.DataFrame] = []
    async for port, payload in recv.process_bigdata(
        ddf=ddf, group_by=["k"], aggregations=aggs, metrics=data_ops_metrics
    ):
        assert port == "out"
        outs.append(payload)

    assert len(outs) == 1
    out_ddf = outs[0]
    out = out_ddf.compute().sort_values("k").reset_index(drop=True)

    expected = (
        pd.DataFrame(
            [
                {"k": "x", "a_min": 1, "a_max": 3, "names": 2, "n": 2},
                {"k": "y", "a_min": 2, "a_max": 8, "names": 2, "n": 2},
            ]
        )
        .sort_values("k")
        .reset_index(drop=True)
    )
    assert list(out.columns) == ["k", "a_min", "a_max", "names", "n"]
    assert_frame_equal(out, expected, check_dtype=False)

    assert data_ops_metrics.lines_received == 4
    assert data_ops_metrics.lines_processed == 2
    assert data_ops_metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_receiver_unsupported_op_raises(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    df = pd.DataFrame([{"x": 1}, {"x": 2}])
    aggs = [{"src": "x", "op": "var", "dest": "v"}]

    recv = AggregationReceiver()

    async def _drain() -> None:
        async for _p, _pl in recv.process_bulk(
            dataframe=df, group_by=[], aggregations=aggs, metrics=data_ops_metrics
        ):
            pass

    with pytest.raises(ValueError, match="Unsupported aggregation 'var'"):
        await _drain()
