from __future__ import annotations

from typing import Dict, List

import dask.dataframe as dd
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from etl_core.components.wiring.column_definition import DataType
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.data_operations_receivers.type_conversion.type_conversion_helper import (
    OnError,
    TypeConversionRule,
)
from etl_core.receivers.data_operations_receivers.type_conversion.type_conversion_receiver import (
    TypeConversionReceiver,
)


@pytest.mark.asyncio
async def test_receiver_process_row__simple_dict() -> None:
    recv = TypeConversionReceiver()
    row: Dict[str, object] = {"age": "10", "name": "Alice"}
    rules = [TypeConversionRule(column_path="age", target=DataType.INTEGER)]
    metrics = ComponentMetrics()

    outs = [item async for item in recv.process_row(row=row, rules=rules, metrics=metrics)]
    assert len(outs) == 1
    port, out_row = outs[0]
    assert port == "out"
    assert out_row["age"] == 10 and out_row["name"] == "Alice"

    assert metrics.lines_received == 1
    assert metrics.lines_forwarded == 1


@pytest.mark.asyncio
async def test_receiver_process_bulk__pandas_df__int_cast() -> None:
    recv = TypeConversionReceiver()
    df = pd.DataFrame({"x": ["1", "2", None]})
    rules = [TypeConversionRule(column_path="x", target=DataType.INTEGER)]
    metrics = ComponentMetrics()

    outs = [item async for item in recv.process_bulk(dataframe=df, rules=rules, metrics=metrics)]
    assert len(outs) == 1
    port, out_df = outs[0]
    assert port == "out"

    expected = pd.DataFrame({"x": [1, 2, pd.NA]}).astype({"x": "Int64"})
    got = out_df.astype({"x": "Int64"})
    assert_frame_equal(got.reset_index(drop=True), expected.reset_index(drop=True), check_dtype=False)

    assert metrics.lines_received == 3
    assert metrics.lines_forwarded == 3


@pytest.mark.asyncio
async def test_receiver_process_bulk__on_error_skip() -> None:
    recv = TypeConversionReceiver()
    df = pd.DataFrame({"flag": ["true", "x"]})
    rules = [
        TypeConversionRule(
            column_path="flag",
            target=DataType.BOOLEAN,
            on_error=OnError.SKIP,
        )
    ]
    metrics = ComponentMetrics()

    outs = [item async for item in recv.process_bulk(dataframe=df, rules=rules, metrics=metrics)]
    assert len(outs) == 1
    port, out_df = outs[0]
    assert port == "out"

    assert list(out_df["flag"]) == [True, "x"]
    assert metrics.lines_received == 2
    assert metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_receiver_process_bigdata__dask_df() -> None:
    recv = TypeConversionReceiver()
    pdf = pd.DataFrame({"v": ["1", "2", "3"]})
    ddf = dd.from_pandas(pdf, npartitions=2)
    rules = [TypeConversionRule(column_path="v", target=DataType.INTEGER)]
    metrics = ComponentMetrics()

    outs = [item async for item in recv.process_bigdata(ddf=ddf, rules=rules, metrics=metrics)]
    assert len(outs) == 1
    port, out_ddf = outs[0]
    assert port == "out"

    got = out_ddf.compute()
    assert list(got["v"].astype("Int64")) == [1, 2, 3]

    assert metrics.lines_received >= 3
    assert metrics.lines_forwarded >= 3