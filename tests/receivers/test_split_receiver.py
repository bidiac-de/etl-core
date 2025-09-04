import asyncio
from datetime import datetime, timedelta
from copy import deepcopy

import dask.dataframe as dd
import pandas as pd
import pytest

from etl_core.components.wiring.ports import OutPortSpec
from etl_core.receivers.data_flow.split_receiver import SplitReceiver
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (
    DataOperationsMetrics,
)



@pytest.mark.asyncio
async def test_map_row_fanout_and_copy(data_ops_metrics: DataOperationsMetrics):
    recv = SplitReceiver()
    ports = (OutPortSpec(name="a"), OutPortSpec(name="b"))
    row = {"id": 1, "name": "Alice", "nested": {"x": 1}}

    outs = []
    async for port, payload in recv.process_row(row=row, branches=ports, metrics=data_ops_metrics):
        outs.append((port, payload))

    # two outputs, port objects returned and payloads are deep copies (fanout>1)
    assert len(outs) == 2
    names = [p.name for p, _ in outs]
    assert set(names) == {"a", "b"}

    # modify original row -> outputs unchanged (deepcopy)
    row["nested"]["x"] = 42
    for _, payload in outs:
        assert payload["nested"]["x"] == 1

    assert data_ops_metrics.lines_received == 1
    assert data_ops_metrics.lines_forwarded == 2
    assert data_ops_metrics.lines_processed == 2


@pytest.mark.asyncio
async def test_map_row_single_fanout_no_copy(data_ops_metrics: DataOperationsMetrics):
    recv = SplitReceiver()
    ports = (OutPortSpec(name="single"),)
    row = {"id": 2, "name": "Bob"}

    outs = []
    async for port, payload in recv.process_row(row=row, branches=ports, metrics=data_ops_metrics):
        outs.append((port, payload))

    assert len(outs) == 1
    _, payload = outs[0]
    # single fanout -> payload may be same object (no deepcopy)
    assert payload is row or payload == row

    assert data_ops_metrics.lines_received >= 1

@pytest.mark.asyncio
async def test_map_row_second_fanout(data_ops_metrics: DataOperationsMetrics):
    recv = SplitReceiver()
    ports = (OutPortSpec(name="single"), OutPortSpec(name="second"))
    row = {"id": 2, "name": "Bob"}

    outs = []
    async for port, payload in recv.process_row(row=row, branches=ports, metrics=data_ops_metrics):
        outs.append((port, payload))

    assert len(outs) == 2
    names = [p.name for p, _ in outs]
    assert set(names) == {"single", "second"}

    # modify original row -> outputs unchanged (deepcopy)
    row["name"] = "Charlie"
    for _, payload in outs:
        assert payload["name"] == "Bob"

    assert data_ops_metrics.lines_received == 1
    assert data_ops_metrics.lines_forwarded == 2
    assert data_ops_metrics.lines_processed == 2


@pytest.mark.asyncio
async def test_map_bulk_copy_behavior_and_metrics(data_ops_metrics: DataOperationsMetrics):
    recv = SplitReceiver()
    ports = (OutPortSpec(name="p1"), OutPortSpec(name="p2"))
    df = pd.DataFrame([{"id": 1}, {"id": 2}])

    outs = []
    async for port, out_df in recv.process_bulk(dataframe=df, branches=ports, metrics=data_ops_metrics):
        outs.append((port, out_df))

    assert len(outs) == 2
    df.loc[0, "id"] = 999
    for _, out_df in outs:
        assert out_df.iloc[0]["id"] == 1

    assert data_ops_metrics.lines_received == 2
    assert data_ops_metrics.lines_forwarded == 4
    assert data_ops_metrics.lines_processed == 4


@pytest.mark.asyncio
async def test_map_bigdata_forward_reference_and_count():
    recv = SplitReceiver()
    ports = (OutPortSpec(name="d1"), OutPortSpec(name="d2"))
    pdf = pd.DataFrame([{"id": i} for i in range(5)])
    ddf = dd.from_pandas(pdf, npartitions=2)

    outs = []
    async for port, out_ddf in recv.process_bigdata(ddf=ddf, branches=ports):
        outs.append((port, out_ddf))

    assert len(outs) == 2
    # receiver forwards same dask object (currently not copied because of dask's lazy evaluation)
    for _, out_ddf in outs:
        assert isinstance(out_ddf, dd.DataFrame)
        assert out_ddf is ddf
        