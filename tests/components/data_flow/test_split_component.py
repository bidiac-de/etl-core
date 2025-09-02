import asyncio
from datetime import datetime, timedelta

import pandas as pd
import pytest

from etl_core.components.wiring.ports import OutPortSpec
from etl_core.components.data_flow.split import SplitComponent
from etl_core.job_execution.job_execution_handler import Out as OutEnvelope
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (
    DataOperationsMetrics,
)


@pytest.fixture
def metrics() -> DataOperationsMetrics:
    return DataOperationsMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_processed=0,
        lines_forwarded=0,
        lines_dismissed=0,
    )


@pytest.mark.asyncio
async def test_split_component_process_row(metrics: DataOperationsMetrics):
    comp = SplitComponent(
        name="split1",
        description="test splitter",
        comp_type="split",
        OUTPUT_PORTS=(OutPortSpec(name="one"), OutPortSpec(name="two")),
    )

    row = {"id": 1, "name": "Alice"}
    outs = []
    async for o in comp.process_row(row=row, metrics=metrics):
        assert isinstance(o, OutEnvelope)
        outs.append(o)

    assert {o.port for o in outs} == {"one", "two"}
    assert metrics.lines_received >= 1
    assert metrics.lines_forwarded >= 2


@pytest.mark.asyncio
async def test_split_component_process_bulk(metrics: DataOperationsMetrics):
    comp = SplitComponent(
        name="split2",
        description="bulk splitter",
        comp_type="split",
        OUTPUT_PORTS=(OutPortSpec(name="A"), OutPortSpec(name="B"),),
    )

    df = pd.DataFrame([{"id": i, "name": f"U{i}"} for i in range(3)])
    outs = []
    async for o in comp.process_bulk(dataframe=df, metrics=metrics):
        outs.append(o)

    # We expect one Out per OUTPUT_PORT with DataFrame payload
    assert {o.port for o in outs} == {"A", "B"}
    for o in outs:
        assert isinstance(o.payload, pd.DataFrame)
        assert len(o.payload) == 3

    assert metrics.lines_received == 3