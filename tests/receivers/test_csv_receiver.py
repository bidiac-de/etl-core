import asyncio
from datetime import datetime, timedelta
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from src.receivers.files.csv.csv_receiver import CSVReceiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics


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
    return Path(__file__).parent.parent / "components" / "data" / "csv" / "test_data.csv"


import asyncio
import pytest

@pytest.mark.asyncio
async def test_readcsv_row(sample_csv_file: Path, metrics: ComponentMetrics):
    r = CSVReceiver()

    async def drain():
        out = []
        async for row in r.read_row(filepath=sample_csv_file, metrics=metrics):
            out.append(row)
        return out

    rows = await asyncio.wait_for(drain(), timeout=3.0)

    assert isinstance(rows, list)
    assert len(rows) >= 1
    assert rows[0]["id"] == "1"
    assert rows[0]["name"] == "Alice"


@pytest.mark.asyncio
async def test_readcsv_bulk(sample_csv_file: Path, metrics: ComponentMetrics):
    r = CSVReceiver()
    df = await r.read_bulk(filepath=sample_csv_file, metrics=metrics)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert set(df.columns) == {"id", "name"}
    assert "Bob" in set(df["name"])


@pytest.mark.asyncio
async def test_readcsv_bigdata(sample_csv_file: Path, metrics: ComponentMetrics):
    r = CSVReceiver()
    ddf = await r.read_bigdata(filepath=sample_csv_file, metrics=metrics)
    assert isinstance(ddf, dd.DataFrame)
    df = ddf.compute()
    assert len(df) == 3
    assert "Charlie" in set(df["name"])


@pytest.mark.asyncio
async def test_writecsv_row(tmp_path: Path, metrics: ComponentMetrics):
    file_path = tmp_path / "out_row.csv"
    r = CSVReceiver()

    await r.write_row(filepath=file_path, metrics=metrics, row={"id": "10", "name": "Daisy"})
    await r.write_row(filepath=file_path, metrics=metrics, row={"id": "11", "name": "Eli"})

    df = await r.read_bulk(filepath=file_path, metrics=metrics)
    assert len(df) == 2
    assert set(df["name"]) == {"Daisy", "Eli"}


@pytest.mark.asyncio
async def test_writecsv_bulk(tmp_path: Path, metrics: ComponentMetrics):
    file_path = tmp_path / "out_bulk.csv"
    file_path.touch()
    r = CSVReceiver()

    data = [
        {"id": "20", "name": "Finn"},
        {"id": "21", "name": "Gina"},
    ]
    await r.write_bulk(filepath=file_path, metrics=metrics, data=data)

    df = await r.read_bulk(filepath=file_path, metrics=metrics)
    assert len(df) == 2
    assert set(df["name"]) == {"Finn", "Gina"}


@pytest.mark.asyncio
async def test_writecsv_bigdata(tmp_path: Path, metrics: ComponentMetrics):
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

    await r.write_bigdata(filepath=file_path, metrics=metrics, data=ddf_in)

    df_out = await r.read_bulk(filepath=file_path, metrics=metrics)
    assert len(df_out) == 2
    assert set(df_out["name"]) == {"Hugo", "Ivy"}