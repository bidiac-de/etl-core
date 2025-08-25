import pandas as pd
import dask.dataframe as dd
import pytest

from datetime import datetime, timedelta
from typing import Any, Dict, List
from pandas.testing import assert_frame_equal


from etl_core.components.data_operations.filter.filter_component import FilterComponent
from etl_core.components.data_operations.filter.comparison_rule import ComparisonRule
from etl_core.metrics.component_metrics.data_operations_metrics.filter_metrics import (
    FilterMetrics,
)


@pytest.fixture
def metrics() -> FilterMetrics:
    return FilterMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
        lines_dismissed=0,
    )


@pytest.mark.asyncio
async def test_filter_component_row(metrics: FilterMetrics) -> None:
    comp = FilterComponent(
        name="Filter rows",
        description="row path",
        comp_type="filter",
        rule=ComparisonRule(column="name", operator="==", value="Alice"),
    )

    rows_in: List[Dict[str, Any]] = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Alice"},
    ]

    out: List[Dict[str, Any]] = []
    for row in rows_in:
        async for r in comp.process_row(row=row, metrics=metrics):
            out.append(r)

    assert [r["id"] for r in out] == [1, 3]
    assert all(r["name"] == "Alice" for r in out)
    assert metrics.lines_dismissed == 1
    assert metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_filter_component_bulk_keeps_rows_meeting_rule(
    metrics: FilterMetrics,
) -> None:
    comp = FilterComponent(
        name="Filter bulk",
        description="bulk path",
        comp_type="filter",
        rule=ComparisonRule(column="id", operator=">=", value=2),
    )

    frames = [
        pd.DataFrame([{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]),
        pd.DataFrame([{"id": 3, "name": "C"}]),
    ]

    parts = []
    for frame in frames:
        parts.extend(
            [df async for df in comp.process_bulk(dataframe=frame, metrics=metrics)]
        )

    out_df = (
        pd.concat(parts, ignore_index=True)
        if parts
        else pd.DataFrame(columns=["id", "name"])
    )

    expected = pd.DataFrame([{"id": 2, "name": "B"}, {"id": 3, "name": "C"}])

    out_df_sorted = out_df.sort_values(["id", "name"]).reset_index(drop=True)
    expected_sorted = expected.sort_values(["id", "name"]).reset_index(drop=True)
    assert_frame_equal(out_df_sorted, expected_sorted)

    assert metrics.lines_dismissed == 1
    assert metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_filter_component_bigdata_filters_expected_names(
    metrics: FilterMetrics,
) -> None:
    comp = FilterComponent(
        name="Filter big",
        description="bigdata path",
        comp_type="filter",
        rule=ComparisonRule(
            logical_operator="OR",
            rules=[
                ComparisonRule(column="name", operator="==", value="Alice"),
                ComparisonRule(column="name", operator="==", value="Charlie"),
            ],
        ),
    )

    pdf = pd.DataFrame(
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]
    )
    ddf = dd.from_pandas(pdf, npartitions=2)

    outs = [d async for d in comp.process_bigdata(ddf, metrics=metrics)]
    assert outs, "FilterComponent yielded no bigdata result"

    result_ddf = dd.concat(outs, axis=0) if len(outs) > 1 else outs[0]
    out = result_ddf.compute().sort_values(["id", "name"]).reset_index(drop=True)

    expected = (
        pd.DataFrame(
            [
                {"id": 1, "name": "Alice"},
                {"id": 3, "name": "Charlie"},
            ]
        )
        .sort_values(["id", "name"])
        .reset_index(drop=True)
    )

    expected = expected.astype({col: out[col].dtype for col in expected.columns})

    assert_frame_equal(out, expected)

    assert metrics.lines_dismissed == 1
    assert metrics.lines_forwarded == 2
