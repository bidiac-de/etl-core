from __future__ import annotations

import pandas as pd
import dask.dataframe as dd
import pytest

from datetime import datetime, timedelta
from typing import Any, Dict, List
from pandas.testing import assert_frame_equal

from etl_core.components.envelopes import Out
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

    passed: List[Dict[str, Any]] = []
    failed: List[Dict[str, Any]] = []

    for row in rows_in:
        async for out in comp.process_row(row=row, metrics=metrics):
            assert isinstance(out, Out)
            if out.port == "pass":
                passed.append(out.payload)
            elif out.port == "fail":
                failed.append(out.payload)
            else:
                pytest.fail(f"Unexpected port {out.port!r}")

    assert [r["id"] for r in passed] == [1, 3]
    assert all(r["name"] == "Alice" for r in passed)
    assert [r["id"] for r in failed] == [2]

    assert metrics.lines_received == 3
    assert metrics.lines_forwarded == 2
    assert metrics.lines_dismissed == 1


@pytest.mark.asyncio
async def test_filter_component_bulk_splits_frames(metrics: FilterMetrics) -> None:
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

    pass_parts: List[pd.DataFrame] = []
    fail_parts: List[pd.DataFrame] = []

    for frame in frames:
        async for out in comp.process_bulk(dataframe=frame, metrics=metrics):
            assert isinstance(out, Out)
            if out.port == "pass":
                pass_parts.append(out.payload)
            elif out.port == "fail":
                fail_parts.append(out.payload)
            else:
                pytest.fail(f"Unexpected port {out.port!r}")

    passed = (
        pd.concat(pass_parts, ignore_index=True)
        if pass_parts
        else pd.DataFrame(columns=["id", "name"])
    )
    failed = (
        pd.concat(fail_parts, ignore_index=True)
        if fail_parts
        else pd.DataFrame(columns=["id", "name"])
    )

    expected_pass = pd.DataFrame([{"id": 2, "name": "B"}, {"id": 3, "name": "C"}])
    expected_fail = pd.DataFrame([{"id": 1, "name": "A"}])

    assert_frame_equal(
        passed.sort_values(["id", "name"]).reset_index(drop=True),
        expected_pass.sort_values(["id", "name"]).reset_index(drop=True),
    )
    assert_frame_equal(
        failed.sort_values(["id", "name"]).reset_index(drop=True),
        expected_fail.sort_values(["id", "name"]).reset_index(drop=True),
    )

    assert metrics.lines_received == 3
    assert metrics.lines_forwarded == 2
    assert metrics.lines_dismissed == 1


@pytest.mark.asyncio
async def test_filter_component_bigdata_splits_ddf(metrics: FilterMetrics) -> None:
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

    pass_ddf = None
    fail_ddf = None

    outs = []
    async for o in comp.process_bigdata(ddf, metrics=metrics):
        outs.append(o)
    for out in outs:
        assert isinstance(out, Out)
        if out.port == "pass":
            pass_ddf = out.payload
        elif out.port == "fail":
            fail_ddf = out.payload

    assert pass_ddf is not None and fail_ddf is not None

    passed = pass_ddf.compute().sort_values(["id", "name"]).reset_index(drop=True)
    failed = fail_ddf.compute().sort_values(["id", "name"]).reset_index(drop=True)

    expected_pass = (
        pd.DataFrame([{"id": 1, "name": "Alice"}, {"id": 3, "name": "Charlie"}])
        .sort_values(["id", "name"])
        .reset_index(drop=True)
    )
    expected_fail = (
        pd.DataFrame([{"id": 2, "name": "Bob"}])
        .sort_values(["id", "name"])
        .reset_index(drop=True)
    )

    assert_frame_equal(passed, expected_pass, check_dtype=False)
    assert_frame_equal(failed, expected_fail, check_dtype=False)

    assert metrics.lines_received == 3
    assert metrics.lines_forwarded == 2
    assert metrics.lines_dismissed == 1
