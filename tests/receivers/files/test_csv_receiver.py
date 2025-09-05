import asyncio
from datetime import datetime, timedelta
import inspect
from pathlib import Path
from typing import AsyncGenerator

import dask.dataframe as dd
import pandas as pd
import pytest

from etl_core.receivers.files.csv.csv_receiver import CSVReceiver
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics


@pytest.fixture
def metrics() -> ComponentMetrics:
    return ComponentMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
    )


@pytest.fixture
def sample_csv_file() -> Path:
    return (
        Path(__file__).parent.parent.parent
        / "components"
        / "data"
        / "csv"
        / "test_data.csv"
    )


@pytest.mark.asyncio
async def test_csvreceiver_read_row_streaming(
    sample_csv_file: Path, metrics: ComponentMetrics
):
    r = CSVReceiver()

    rows = r.read_row(filepath=sample_csv_file, metrics=metrics, separator=",")

    assert inspect.isasyncgen(rows) or isinstance(rows, AsyncGenerator)

    first = await asyncio.wait_for(anext(rows), timeout=0.25)
    assert set(first.keys()) == {"id", "name"}
    assert first["id"] == "1"
    assert first["name"] == "Alice"

    second = await asyncio.wait_for(anext(rows), timeout=0.25)
    assert set(second.keys()) == {"id", "name"}
    assert second["id"] == "2"
    assert second["name"] == "Bob"

    await rows.aclose()


@pytest.mark.asyncio
async def test_read_csv_bulk(sample_csv_file: Path, metrics: ComponentMetrics):
    r = CSVReceiver()
    df = await r.read_bulk(filepath=sample_csv_file, metrics=metrics, separator=",")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert set(df.columns) == {"id", "name"}
    assert "Bob" in set(df["name"])


@pytest.mark.asyncio
async def test_read_csv_bigdata(sample_csv_file: Path, metrics: ComponentMetrics):
    r = CSVReceiver()
    ddf = await r.read_bigdata(filepath=sample_csv_file, metrics=metrics, separator=",")
    assert isinstance(ddf, dd.DataFrame)
    df = ddf.compute()
    assert len(df) == 3
    assert "Charlie" in set(df["name"])


@pytest.mark.asyncio
async def test_write_csv_row(tmp_path: Path, metrics: ComponentMetrics):
    file_path = tmp_path / "out_row.csv"
    r = CSVReceiver()

    await r.write_row(
        filepath=file_path,
        metrics=metrics,
        row={"id": "10", "name": "Daisy"},
        separator=",",
    )

    assert file_path.exists()
    lines = file_path.read_text().splitlines()
    assert lines[0] == "id,name"
    assert lines[1] == "10,Daisy"

    await r.write_row(
        filepath=file_path,
        metrics=metrics,
        row={"id": "11", "name": "Eli"},
        separator=",",
    )

    assert file_path.exists()
    lines = file_path.read_text().splitlines()
    assert lines[0] == "id,name"
    assert lines[1] == "10,Daisy"
    assert lines[2] == "11,Eli"


@pytest.mark.asyncio
async def test_write_csv_bulk(tmp_path: Path, metrics: ComponentMetrics):
    file_path = tmp_path / "out_bulk.csv"
    file_path.touch()
    r = CSVReceiver()

    data = [
        {"id": 20, "name": "Finn"},
        {"id": 21, "name": "Gina"},
    ]
    df = pd.DataFrame(data)

    await r.write_bulk(filepath=file_path, metrics=metrics, data=df, separator=",")

    assert file_path.exists()
    df_out = pd.read_csv(file_path)
    pd.testing.assert_frame_equal(df_out, df)


@pytest.mark.asyncio
async def test_write_csv_bigdata(tmp_path: Path, metrics: ComponentMetrics):
    file_path = tmp_path / "out_big.csv"
    file_path.touch()
    r = CSVReceiver()

    pdf = pd.DataFrame(
        [
            {"id": 30, "name": "Hugo"},
            {"id": 31, "name": "Ivy"},
        ]
    )
    ddf_in = dd.from_pandas(pdf, npartitions=1)

    await r.write_bigdata(
        filepath=file_path, metrics=metrics, data=ddf_in, separator=","
    )

    assert file_path.exists()

    df_out = pd.read_csv(file_path)
    pd.testing.assert_frame_equal(df_out, pdf)


@pytest.mark.asyncio
async def test_csvreceiver_read_row_missing_file_raises(
    metrics: ComponentMetrics, tmp_path: Path
):
    r = CSVReceiver()
    rows = r.read_row(filepath=tmp_path / "missing.csv", metrics=metrics, separator=",")
    with pytest.raises(FileNotFoundError):
        await anext(rows)
