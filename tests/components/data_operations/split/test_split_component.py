import asyncio
from datetime import datetime, timedelta

import pandas as pd
import pytest

from etl_core.components.wiring.ports import OutPortSpec
from etl_core.components.data_operations.split.split import SplitComponent
from etl_core.job_execution.job_execution_handler import Out as OutEnvelope
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (
    DataOperationsMetrics,
)


@pytest.mark.asyncio
async def test_split_component_process_row(data_ops_metrics: DataOperationsMetrics):
    comp = SplitComponent(
        name="split1",
        description="test splitter",
        comp_type="split",
        OUTPUT_PORTS=(OutPortSpec(name="one"), OutPortSpec(name="two")),
    )

    row = {"id": 1, "name": "Alice"}
    outs = []
    async for o in comp.process_row(row=row, metrics=data_ops_metrics):
        assert isinstance(o, OutEnvelope)
        outs.append(o)

    assert {o.port for o in outs} == {"one", "two"}
    assert data_ops_metrics.lines_received >= 1
    assert data_ops_metrics.lines_forwarded >= 2


@pytest.mark.asyncio
async def test_split_component_process_bulk(data_ops_metrics: DataOperationsMetrics):
    comp = SplitComponent(
        name="split2",
        description="bulk splitter",
        OUTPUT_PORTS=(OutPortSpec(name="A"), OutPortSpec(name="B")),
    )

    df = pd.DataFrame([{"id": i, "name": f"U{i}"} for i in range(3)])
    outs = []
    async for o in comp.process_bulk(dataframe=df, metrics=data_ops_metrics):
        outs.append(o)

    # We expect one Out per OUTPUT_PORT with DataFrame payload
    assert {o.port for o in outs} == {"A", "B"}
    for o in outs:
        assert isinstance(o.payload, pd.DataFrame)
        assert len(o.payload) == 3

    assert data_ops_metrics.lines_received == 3