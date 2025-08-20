import pandas as pd
import dask.dataframe as dd
import pytest

from datetime import datetime, timedelta
from typing import Any, Dict, List

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


@pytest.mark.asyncio
async def test_filter_component_bulk(metrics: FilterMetrics) -> None:
    comp = FilterComponent(
        name="Filter bulk",
        description="bulk path",
        comp_type="filter",
        rule=ComparisonRule(column="id", operator=">=", value=2),
    )

    f1 = pd.DataFrame([{"id": 1, "name": "A"}, {"id": 2, "name": "B"}])
    f2 = pd.DataFrame([{"id": 3, "name": "C"}])

    parts = []
    for frame in (f1, f2):
        async for df in comp.process_bulk(dataframe=frame, metrics=metrics):
            parts.append(df)

    out_df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    assert isinstance(out_df, pd.DataFrame)
    assert list(out_df["id"]) == [2, 3]


@pytest.mark.asyncio
async def test_filter_component_bigdata(metrics: FilterMetrics) -> None:
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

    outs = []
    async for d in comp.process_bigdata(ddf, metrics=metrics):
        outs.append(d)
    assert outs, "FilterComponent yielded no bigdata result"

    out = outs[0].compute().sort_values("id")
    assert list(out["name"]) == ["Alice", "Charlie"]
