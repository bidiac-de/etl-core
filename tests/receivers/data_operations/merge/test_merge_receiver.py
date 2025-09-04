from __future__ import annotations

from typing import List, Tuple

import dask.dataframe as dd
import pandas as pd
import pytest

from etl_core.components.wiring.ports import OutPortSpec
from etl_core.receivers.data_operations_receivers.merge.merge_receiver import (
    MergeReceiver,
)
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa E501
    DataOperationsMetrics,
)


@pytest.mark.asyncio
async def test_merge_receiver_process_row_forwards_and_metrics(
    data_ops_metrics: DataOperationsMetrics,
):
    recv = MergeReceiver()
    out_port = OutPortSpec(name="out")
    row = {"id": 1, "name": "Alice"}

    outs: List[Tuple[OutPortSpec, dict]] = []
    async for port_spec, payload in recv.process_row(
        out_port=out_port, row=row, metrics=data_ops_metrics
    ):
        outs.append((port_spec, payload))

    assert len(outs) == 1
    assert outs[0][0].name == "out"
    # payload forwarded as-is
    assert outs[0][1] == row
    assert data_ops_metrics.lines_received == 1
    assert data_ops_metrics.lines_processed == 1
    assert data_ops_metrics.lines_forwarded == 1


@pytest.mark.asyncio
async def test_merge_receiver_process_bulk_copies_dataframe_and_updates_metrics(
    data_ops_metrics: DataOperationsMetrics,
):
    recv = MergeReceiver()
    out_port = OutPortSpec(name="out")
    df = pd.DataFrame([{"x": 1}, {"x": 2}])

    outs = []
    async for port_spec, out_df in recv.process_bulk(
        out_port=out_port, dataframe=df, metrics=data_ops_metrics
    ):
        outs.append((port_spec, out_df))

    assert len(outs) == 1
    assert outs[0][0].name == "out"
    # Ensure a copy was forwarded (mutating original should not change forwarded copy)
    df.loc[0, "x"] = 999
    assert outs[0][1].iloc[0]["x"] == 1

    assert data_ops_metrics.lines_received == 2
    assert data_ops_metrics.lines_processed == 2
    assert data_ops_metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_merge_receiver_process_bigdata_forwards_same_ddf_and_counts(
    data_ops_metrics: DataOperationsMetrics,
):
    recv = MergeReceiver()
    out_port = OutPortSpec(name="out")
    pdf = pd.DataFrame([{"i": i} for i in range(5)])
    ddf = dd.from_pandas(pdf, npartitions=2)

    outs = []
    async for port_spec, out_ddf in recv.process_bigdata(
        out_port=out_port, ddf=ddf, metrics=data_ops_metrics
    ):
        outs.append((port_spec, out_ddf))

    assert len(outs) == 1
    assert outs[0][0].name == "out"
    # Dask object should be forwarded by reference
    assert outs[0][1] is ddf
