from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

import dask.dataframe as dd
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from etl_core.components.data_operations.filter.comparison_rule import ComparisonRule
from etl_core.metrics.component_metrics.data_operations_metrics.filter_metrics import (
    FilterMetrics,
)
from etl_core.receivers.data_operations_receivers.filter.filter_receiver import (
    FilterReceiver,
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


async def _collect_rows_split(
    recv: FilterReceiver,
    rows: List[Dict[str, Any]],
    rule: ComparisonRule,
    metrics: FilterMetrics,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Feed rows one-by-one and split outputs by port ('pass' / 'fail').
    Receiver yields (port, payload) tuples now.
    """
    passed: List[Dict[str, Any]] = []
    failed: List[Dict[str, Any]] = []
    for row in rows:
        async for port, payload in recv.process_row(
            row=row, rule=rule, metrics=metrics
        ):
            if port == "pass":
                passed.append(payload)
            elif port == "fail":
                failed.append(payload)
            else:
                pytest.fail(f"Unexpected port {port!r}")
    return passed, failed


@pytest.mark.asyncio
async def test_filter_receiver_row_simple_equality(
    metrics: FilterMetrics,
) -> None:
    rows = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Alice"},
    ]
    rule = ComparisonRule(column="name", operator="==", value="Alice")

    recv = FilterReceiver()
    passed, failed = await _collect_rows_split(recv, rows, rule, metrics)

    assert [r["id"] for r in passed] == [1, 3]
    assert all(r["name"] == "Alice" for r in passed)
    assert [r["id"] for r in failed] == [2]

    assert metrics.lines_dismissed == 1
    assert metrics.lines_forwarded == 2
    assert metrics.lines_received == 3


@pytest.mark.asyncio
async def test_filter_receiver_row_nested_and_or(metrics: FilterMetrics) -> None:
    rows = [
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 2, "name": "Bob", "age": 30},
        {"id": 3, "name": "Charlie", "age": 19},
    ]

    rule = ComparisonRule(
        logical_operator="AND",
        rules=[
            ComparisonRule(
                logical_operator="OR",
                rules=[
                    ComparisonRule(column="name", operator="==", value="Alice"),
                    ComparisonRule(column="name", operator="==", value="Bob"),
                ],
            ),
            ComparisonRule(column="age", operator=">=", value=25),
        ],
    )

    recv = FilterReceiver()
    passed, failed = await _collect_rows_split(recv, rows, rule, metrics)

    assert [r["id"] for r in passed] == [1, 2]
    assert [r["id"] for r in failed] == [3]

    assert metrics.lines_dismissed == 1
    assert metrics.lines_forwarded == 2
    assert metrics.lines_received == 3


@pytest.mark.asyncio
async def test_filter_receiver_bulk_contains_and_not(
    metrics: FilterMetrics,
) -> None:
    df1 = pd.DataFrame([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
    df2 = pd.DataFrame([{"id": 3, "name": "Charlie"}, {"id": 4, "name": "Liam"}])

    rule = ComparisonRule(
        logical_operator="NOT",
        rules=[ComparisonRule(column="name", operator="contains", value="li")],
    )

    recv = FilterReceiver()

    pass_parts: List[pd.DataFrame] = []
    fail_parts: List[pd.DataFrame] = []

    for df in (df1, df2):
        async for port, payload in recv.process_bulk(
            dataframe=df, rule=rule, metrics=metrics
        ):
            if port == "pass":
                pass_parts.append(payload)
            elif port == "fail":
                fail_parts.append(payload)
            else:
                pytest.fail(f"Unexpected port {port!r}")

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

    # NOT contains 'li' keeps only 'Bob'
    assert passed.shape[0] == 1
    assert passed.iloc[0]["name"] == "Bob"
    assert failed.shape[0] == 3

    assert metrics.lines_dismissed == 3
    assert metrics.lines_forwarded == 1
    assert metrics.lines_received == 4


@pytest.mark.asyncio
async def test_filter_receiver_bulk_gt(metrics: FilterMetrics) -> None:
    df = pd.DataFrame([{"id": 1}, {"id": 2}, {"id": 3}])
    rule = ComparisonRule(column="id", operator=">", value=1)

    recv = FilterReceiver()
    pass_parts: List[pd.DataFrame] = []
    fail_parts: List[pd.DataFrame] = []

    async for port, payload in recv.process_bulk(
        dataframe=df, rule=rule, metrics=metrics
    ):
        if port == "pass":
            pass_parts.append(payload)
        elif port == "fail":
            fail_parts.append(payload)
        else:
            pytest.fail(f"Unexpected port {port!r}")

    passed = pd.concat(pass_parts, ignore_index=True)
    failed = (
        pd.concat(fail_parts, ignore_index=True)
        if fail_parts
        else pd.DataFrame(columns=["id"])
    )

    assert list(passed["id"]) == [2, 3]
    assert list(failed["id"]) == [1]

    assert metrics.lines_dismissed == 1
    assert metrics.lines_forwarded == 2
    assert metrics.lines_received == 3


@pytest.mark.asyncio
async def test_filter_receiver_bigdata_map_partitions(
    metrics: FilterMetrics,
) -> None:
    pdf = pd.DataFrame(
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]
    )
    ddf = dd.from_pandas(pdf, npartitions=2)
    rule = ComparisonRule(column="name", operator="contains", value="li")

    recv = FilterReceiver()
    pass_ddf = None
    fail_ddf = None
    async for port, payload in recv.process_bigdata(ddf, metrics=metrics, rule=rule):
        if port == "pass":
            pass_ddf = payload
        elif port == "fail":
            fail_ddf = payload
        else:
            pytest.fail(f"Unexpected port {port!r}")

    assert pass_ddf is not None and fail_ddf is not None

    # Compute and compare results (dtype-agnostic)
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

    assert metrics.lines_dismissed == 1
    assert metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_filter_receiver_bulk_no_matches(metrics: FilterMetrics) -> None:
    df = pd.DataFrame(
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]
    )

    rule = ComparisonRule(column="id", operator=">", value=100)

    recv = FilterReceiver()
    pass_parts: List[pd.DataFrame] = []
    fail_parts: List[pd.DataFrame] = []

    async for port, payload in recv.process_bulk(
        dataframe=df, rule=rule, metrics=metrics
    ):
        if port == "pass":
            pass_parts.append(payload)
        elif port == "fail":
            fail_parts.append(payload)
        else:
            pytest.fail(f"Unexpected port {port!r}")

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

    assert passed.empty
    assert failed.shape[0] == 3

    assert metrics.lines_forwarded == 0
    assert metrics.lines_dismissed == 3
    assert metrics.lines_received == 3
