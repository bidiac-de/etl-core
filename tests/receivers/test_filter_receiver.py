import pandas as pd
import dask.dataframe as dd
import pytest

from datetime import datetime, timedelta
from typing import Any, Dict, List

from etl_core.metrics.component_metrics.data_operations_metrics.filter_metrics import (
    FilterMetrics,
)
from etl_core.receivers.data_operations_receivers.filter.filter_receiver import (
    FilterReceiver,
)
from etl_core.components.data_operations.filter.comparison_rule import ComparisonRule


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


async def _collect_rows(
    recv: FilterReceiver,
    rows: List[Dict[str, Any]],
    rule: ComparisonRule,
    metrics: FilterMetrics,
) -> List[Dict[str, Any]]:
    """
    Feed rows one-by-one into the receiver and collect yielded rows.
    """
    out: List[Dict[str, Any]] = []
    for row in rows:
        async for r in recv.process_row(row=row, rule=rule, metrics=metrics):
            out.append(r)
    return out


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
    out = await _collect_rows(recv, rows, rule, metrics)

    assert [r["id"] for r in out] == [1, 3]
    assert all(r["name"] == "Alice" for r in out)

    assert metrics.lines_dismissed == 1
    assert metrics.lines_forwarded == 2


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
    out = await _collect_rows(recv, rows, rule, metrics)

    assert [r["id"] for r in out] == [1, 2]

    assert metrics.lines_dismissed == 1
    assert metrics.lines_forwarded == 2


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

    parts = []
    for df in (df1, df2):
        async for f in recv.process_bulk(dataframe=df, rule=rule, metrics=metrics):
            parts.append(f)

    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    assert out.shape[0] == 1
    assert out.iloc[0]["name"] == "Bob"

    assert metrics.lines_dismissed == 3
    assert metrics.lines_forwarded == 1


@pytest.mark.asyncio
async def test_filter_receiver_bulk_gt(metrics: FilterMetrics) -> None:
    df = pd.DataFrame([{"id": 1}, {"id": 2}, {"id": 3}])
    rule = ComparisonRule(column="id", operator=">", value=1)

    recv = FilterReceiver()
    parts = []
    async for f in recv.process_bulk(dataframe=df, rule=rule, metrics=metrics):
        parts.append(f)
    out = pd.concat(parts, ignore_index=True)

    assert list(out["id"]) == [2, 3]

    assert metrics.lines_dismissed == 1
    assert metrics.lines_forwarded == 2


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
    parts = []
    async for d in recv.process_bigdata(ddf, metrics=metrics, rule=rule):
        parts.append(d)
    assert parts, "No bigdata result from receiver"

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
    parts = []
    async for f in recv.process_bulk(dataframe=df, rule=rule, metrics=metrics):
        parts.append(f)

    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    assert out.empty
    assert out.shape[0] == 0

    assert metrics.lines_forwarded == 0
    assert metrics.lines_dismissed == 3
