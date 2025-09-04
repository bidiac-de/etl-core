from __future__ import annotations

from typing import List

import pandas as pd
import pytest

from etl_core.components.data_operations.merge.merge import MergeComponent
from etl_core.job_execution.job_execution_handler import Out as OutEnvelope
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (
    DataOperationsMetrics,
)
from etl_core.components.wiring.ports import OutPortSpec, InPortSpec


@pytest.mark.asyncio
async def test_merge_component_process_row_forwards_to_single_output(
    data_ops_metrics: DataOperationsMetrics,
):
    # Define explicit input & output ports
    in_port = InPortSpec(name="in", required=True, fanin="many")
    out_port = OutPortSpec(name="out")

    comp = MergeComponent(
        name="merge1",
        description="test merge",
        comp_type="merge",
        OUTPUT_PORTS=(out_port,),
        extra_input_ports=(in_port,),
    )

    row = {"id": 1, "val": "X"}
    outs: List[OutEnvelope] = []
    async for o in comp.process_row(row=row, metrics=data_ops_metrics):
        outs.append(o)

    # Component should forward every incoming row to the single output port
    assert len(outs) == 1
    assert outs[0].port == "out"
    assert outs[0].payload == row
    assert data_ops_metrics.lines_received == 1
    assert data_ops_metrics.lines_forwarded == 1


@pytest.mark.asyncio
async def test_merge_component_process_bulk_forwards_dataframe_copy(
    data_ops_metrics: DataOperationsMetrics,
):
    in_port = InPortSpec(name="in", required=True, fanin="many")
    out_port = OutPortSpec(name="merged")

    comp = MergeComponent(
        name="merge2",
        description="test merge bulk",
        comp_type="merge",
        OUTPUT_PORTS=(out_port,),
        extra_input_ports=(in_port,),
    )

    df = pd.DataFrame([{"k": 1}, {"k": 2}])
    outs: List[OutEnvelope] = []
    async for o in comp.process_bulk(dataframe=df, metrics=data_ops_metrics):
        outs.append(o)

    # Should forward the DataFrame as-is
    assert len(outs) == 1
    assert outs[0].port == "merged"
    assert isinstance(outs[0].payload, pd.DataFrame)
    df.loc[0, "k"] = 999
    assert outs[0].payload.iloc[0]["k"] == 1

    # Metrics should match number of rows
    assert data_ops_metrics.lines_received == 2
    assert data_ops_metrics.lines_forwarded == 2