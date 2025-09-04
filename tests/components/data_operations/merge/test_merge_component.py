from __future__ import annotations

from datetime import datetime, timedelta
from typing import List

import pandas as pd
import pytest

from etl_core.components.data_operations.merge.merge import MergeComponent
from etl_core.job_execution.job_execution_handler import Out as OutEnvelope
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (
    DataOperationsMetrics,
)
from etl_core.components.wiring.ports import OutPortSpec


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
async def test_merge_component_process_row_forwards_to_single_output(metrics: DataOperationsMetrics):
    # configure component with two inputs routed to single output "out"
    comp = MergeComponent(
        name="merge1",
        description="test merge",
        comp_type="merge",
        INPUT_PORTS=["a", "b"],
        OUTPUT_PORT="out",
    )

    row = {"id": 1, "val": "X"}
    outs: List[OutEnvelope] = []
    async for o in comp.process_row(row=row, metrics=metrics):
        outs.append(o)

    # Merge component should forward every incoming payload to the single output port
    assert len(outs) == 1
    assert outs[0].port == "out"
    assert outs[0].payload == row
    assert metrics.lines_received == 1
    assert metrics.lines_forwarded == 1


@pytest.mark.asyncio
async def test_merge_component_process_bulk_forwards_dataframe_copy(metrics: DataOperationsMetrics):
    comp = MergeComponent(
        name="merge2",
        description="test merge bulk",
        comp_type="merge",
        INPUT_PORTS=["src1", "src2"],
        OUTPUT_PORT="merged",
    )

    df = pd.DataFrame([{"k": 1}, {"k": 2}])
    outs = []
    async for o in comp.process_bulk(dataframe=df, metrics=metrics):
        outs.append(o)

    assert len(outs) == 1
    assert outs[0].port == "merged"
    # ensure forwarded payload is a DataFrame and is a copy (mutating original should not affect forwarded)
    assert isinstance(outs[0].payload, pd.DataFrame)
    df.loc[0, "k"] = 999
    assert outs[0].payload.iloc[0]["k"] == 1

    assert metrics.lines_received == 2
    assert metrics.lines_forwarded == 2