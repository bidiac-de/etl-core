from __future__ import annotations

from typing import Any, Dict, List

import dask.dataframe as dd
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from etl_core.components.data_operations.aggregation.aggregation_component import (  # noqa: E501
    AggregationComponent,
    AggOp,
)
from etl_core.components.envelopes import Out
from etl_core.job_execution.job_execution_handler import InTagged
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa: E501
    DataOperationsMetrics,
)


@pytest.mark.asyncio
async def test_component_row_buffer_and_flush(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    comp = AggregationComponent(
        name="agg-row",
        description="row",
        comp_type="aggregation",
        group_by=["g"],
        aggregations=[
            AggOp(src="v", op="sum", dest="sum_v"),
            AggOp(src="*", op="count", dest="n"),
        ],
    )

    rows: List[Dict[str, Any]] = [
        {"g": "A", "v": 1},
        {"g": "A", "v": 2},
        {"g": "B", "v": 10},
    ]

    for r in rows:
        async for _ in comp.process_row(
            row=InTagged(in_port="in", payload=r), metrics=data_ops_metrics
        ):
            pytest.fail("No output expected before flush")

    outs: List[Out] = []
    async for o in comp.process_row(
        row=InTagged(in_port="in", payload=Ellipsis), metrics=data_ops_metrics
    ):
        outs.append(o)

    assert len(outs) > 0
    assert all(o.port == "out" for o in outs)

    got = (
        pd.DataFrame([o.payload for o in outs]).sort_values("g").reset_index(drop=True)
    )
    expected = (
        pd.DataFrame(
            [
                {"g": "A", "sum_v": 3, "n": 2},
                {"g": "B", "sum_v": 10, "n": 1},
            ]
        )
        .sort_values("g")
        .reset_index(drop=True)
    )
    assert_frame_equal(got, expected, check_dtype=False)

    assert data_ops_metrics.lines_received == 3
    assert data_ops_metrics.lines_forwarded == 2
    assert data_ops_metrics.lines_processed == 2


@pytest.mark.asyncio
async def test_component_bulk_concat_and_flush(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    comp = AggregationComponent(
        name="agg-bulk",
        description="bulk",
        comp_type="aggregation",
        group_by=["k"],
        aggregations=[AggOp(src="v", op="mean", dest="avg_v")],
    )

    df1 = pd.DataFrame([{"k": "x", "v": 1}, {"k": "x", "v": 3}])
    df2 = pd.DataFrame([{"k": "y", "v": 10}])

    async for _ in comp.process_bulk(
        dataframe=InTagged(in_port="in", payload=df1), metrics=data_ops_metrics
    ):
        pytest.fail("No output expected before flush")
    async for _ in comp.process_bulk(
        dataframe=InTagged(in_port="in", payload=df2), metrics=data_ops_metrics
    ):
        pytest.fail("No output expected before flush")

    outs: List[Out] = []
    async for o in comp.process_bulk(
        dataframe=InTagged(in_port="in", payload=Ellipsis), metrics=data_ops_metrics
    ):
        outs.append(o)
    assert len(outs) == 1 and outs[0].port == "out"

    out_df = outs[0].payload.sort_values("k").reset_index(drop=True)
    expected = (
        pd.DataFrame(
            [
                {"k": "x", "avg_v": 2.0},
                {"k": "y", "avg_v": 10.0},
            ]
        )
        .sort_values("k")
        .reset_index(drop=True)
    )
    assert_frame_equal(out_df, expected, check_dtype=False)

    assert data_ops_metrics.lines_received == 3
    assert data_ops_metrics.lines_forwarded == 2
    assert data_ops_metrics.lines_processed == 2


@pytest.mark.asyncio
async def test_component_bigdata_flush_empty_buffer_returns_empty_ddf(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    comp = AggregationComponent(
        name="agg-bigdata-empty",
        description="big",
        comp_type="aggregation",
        group_by=["k"],
        aggregations=[AggOp(src="v", op="sum", dest="sum_v")],
    )

    outs: List[Out] = []
    async for o in comp.process_bigdata(
        ddf=InTagged(in_port="in", payload=Ellipsis), metrics=data_ops_metrics
    ):
        outs.append(o)
    assert len(outs) == 1 and outs[0].port == "out"

    out_ddf = outs[0].payload
    out_pdf = out_ddf.compute()
    assert out_pdf.shape[0] == 0
    assert isinstance(out_pdf, pd.DataFrame)


@pytest.mark.asyncio
async def test_component_bigdata_groupby_multi_aggs_and_count(
    data_ops_metrics: DataOperationsMetrics,
) -> None:
    comp = AggregationComponent(
        name="agg-bigdata",
        description="big",
        comp_type="aggregation",
        group_by=["k"],
        aggregations=[
            AggOp(src="v", op="min", dest="v_min"),
            AggOp(src="v", op="max", dest="v_max"),
            AggOp(src="*", op="count", dest="n"),
        ],
    )

    pdf = pd.DataFrame(
        [
            {"k": "x", "v": 1},
            {"k": "x", "v": 4},
            {"k": "y", "v": 2},
            {"k": "y", "v": 8},
        ]
    )
    ddf = dd.from_pandas(pdf, npartitions=2)

    async for _ in comp.process_bigdata(
        ddf=InTagged(in_port="in", payload=ddf), metrics=data_ops_metrics
    ):
        pytest.fail("No output expected before flush")

    outs: List[Out] = []
    async for o in comp.process_bigdata(
        ddf=InTagged(in_port="in", payload=Ellipsis), metrics=data_ops_metrics
    ):
        outs.append(o)
    assert len(outs) == 1 and outs[0].port == "out"

    out_pdf = outs[0].payload.compute().sort_values("k").reset_index(drop=True)
    expected = (
        pd.DataFrame(
            [
                {"k": "x", "v_min": 1, "v_max": 4, "n": 2},
                {"k": "y", "v_min": 2, "v_max": 8, "n": 2},
            ]
        )
        .sort_values("k")
        .reset_index(drop=True)
    )
    assert_frame_equal(out_pdf, expected, check_dtype=False)

    assert data_ops_metrics.lines_received == 4
    assert data_ops_metrics.lines_processed == 2
    assert data_ops_metrics.lines_forwarded == 2
