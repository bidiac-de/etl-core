import asyncio
from datetime import datetime, timedelta
from typing import Any, AsyncIterator, Dict, List

import dask.dataframe as dd
import pandas as pd
import pytest

from src.components.data_operations.filter.filter_component import FilterComponent
from src.components.data_operations.filter.comparison_rule import ComparisonRule
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.strategies.row_strategy import RowExecutionStrategy
from src.strategies.bulk_strategy import BulkExecutionStrategy
from src.strategies.bigdata_strategy import BigDataExecutionStrategy


async def agen_rows(items: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
    for it in items:
        yield it
        await asyncio.sleep(0)


async def aframes(items: List[pd.DataFrame]) -> AsyncIterator[pd.DataFrame]:
    for it in items:
        yield it
        await asyncio.sleep(0)


@pytest.fixture(autouse=True)
def patch_strategies(monkeypatch):
    """
    Make strategies return easy-to-assert shapes in tests:
      - Row: list[dict]
      - Bulk: single concatenated DataFrame
      - BigData: dd.DataFrame
    """

    async def row_exec(self, component, payload, metrics):
        return [r async for r in component.process_row(payload, metrics=metrics)]

    async def bulk_exec(self, component, payload, metrics):
        parts = [f async for f in component.process_bulk(payload, metrics=metrics)]
        import pandas as pd

        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    async def bigdata_exec(self, component, payload, metrics):
        return await component.process_bigdata(payload, metrics=metrics)

    monkeypatch.setattr(RowExecutionStrategy, "execute", row_exec, raising=True)
    monkeypatch.setattr(BulkExecutionStrategy, "execute", bulk_exec, raising=True)
    monkeypatch.setattr(BigDataExecutionStrategy, "execute", bigdata_exec, raising=True)


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
async def test_filter_component_row(metrics: ComponentMetrics):
    comp = FilterComponent(
        name="Filter rows",
        description="row path",
        comp_type="filter",
        rule=ComparisonRule(column="name", operator="==", value="Alice"),
    )
    comp.strategy = RowExecutionStrategy()

    rows_in = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Alice"},
    ]
    out = await comp.execute(payload=agen_rows(rows_in), metrics=metrics)
    assert [r["id"] for r in out] == [1, 3]


@pytest.mark.asyncio
async def test_filter_component_bulk(metrics: ComponentMetrics):
    comp = FilterComponent(
        name="Filter bulk",
        description="bulk path",
        comp_type="filter",
        rule=ComparisonRule(column="id", operator=">=", value=2),
    )
    comp.strategy = BulkExecutionStrategy()

    f1 = pd.DataFrame([{"id": 1, "name": "A"}, {"id": 2, "name": "B"}])
    f2 = pd.DataFrame([{"id": 3, "name": "C"}])
    out_df = await comp.execute(payload=aframes([f1, f2]), metrics=metrics)

    assert isinstance(out_df, pd.DataFrame)
    assert list(out_df["id"]) == [2, 3]


@pytest.mark.asyncio
async def test_filter_component_bigdata(metrics: ComponentMetrics):
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

    async for dout in comp.process_bigdata(ddf, metrics=metrics):
        out = dout.compute().sort_values("id")
        assert list(out["name"]) == ["Alice", "Charlie"]
        break
