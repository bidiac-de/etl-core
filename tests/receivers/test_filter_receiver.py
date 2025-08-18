import asyncio
from datetime import datetime, timedelta
from typing import Any, AsyncIterator, Dict, List

import dask.dataframe as dd
import pandas as pd
import pytest

from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.data_operations_receivers.filter_receiver import FilterReceiver
from src.components.data_operations.filter.comparison_rule import ComparisonRule


async def agen_from_list(items: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
    for it in items:
        yield it
        await asyncio.sleep(0)


async def aframes_from_list(frames: List[pd.DataFrame]) -> AsyncIterator[pd.DataFrame]:
    for f in frames:
        yield f
        await asyncio.sleep(0)


@pytest.fixture
def metrics() -> ComponentMetrics:
    return ComponentMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
    )


@pytest.mark.asyncio
async def test_filter_receiver_row_simple_equality(metrics: ComponentMetrics):
    rows = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Alice"},
    ]
    rule = ComparisonRule(column="name", operator="==", value="Alice")

    recv = FilterReceiver()
    out = [
        r
        async for r in recv.process_row(
            agen_from_list(rows), metrics=metrics, rule=rule
        )
    ]

    assert [r["id"] for r in out] == [1, 3]
    assert all(r["name"] == "Alice" for r in out)


@pytest.mark.asyncio
async def test_filter_receiver_row_nested_and_or(metrics: ComponentMetrics):
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
    out = [
        r
        async for r in recv.process_row(
            agen_from_list(rows), metrics=metrics, rule=rule
        )
    ]

    assert [r["id"] for r in out] == [1, 2]


@pytest.mark.asyncio
async def test_filter_receiver_bulk_contains_and_not(metrics: ComponentMetrics):
    df1 = pd.DataFrame([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
    df2 = pd.DataFrame([{"id": 3, "name": "Charlie"}, {"id": 4, "name": "Liam"}])

    rule = ComparisonRule(
        logical_operator="NOT",
        rules=[ComparisonRule(column="name", operator="contains", value="li")],
    )

    recv = FilterReceiver()
    parts = [
        f
        async for f in recv.process_bulk(
            aframes_from_list([df1, df2]), metrics=metrics, rule=rule
        )
    ]
    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    assert out.shape[0] == 1
    assert out.iloc[0]["name"] == "Bob"


@pytest.mark.asyncio
async def test_filter_receiver_bulk_gt(metrics: ComponentMetrics):
    df = pd.DataFrame([{"id": 1}, {"id": 2}, {"id": 3}])
    rule = ComparisonRule(column="id", operator=">", value=1)

    recv = FilterReceiver()
    parts = [
        f
        async for f in recv.process_bulk(
            aframes_from_list([df]), metrics=metrics, rule=rule
        )
    ]
    out = pd.concat(parts, ignore_index=True)

    assert list(out["id"]) == [2, 3]


@pytest.mark.asyncio
async def test_filter_receiver_bigdata_map_partitions(metrics: ComponentMetrics):
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
    parts = [d async for d in recv.process_bigdata(ddf, metrics=metrics, rule=rule)]
    assert parts, "No bigdata result from receiver"
    dout = parts[0]

    out = dout.compute().sort_values("id")
    assert list(out["id"]) == [1, 3]
