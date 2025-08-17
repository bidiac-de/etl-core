import asyncio
from datetime import datetime, timedelta
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from src.receivers.files.excel.excel_receiver import ExcelReceiver
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
def sample_excel_file() -> Path:
    return Path(__file__).parent.parent / "components" / "files" / "data" / "test_data.xlsx"


@pytest.mark.asyncio
async def test_readexcel_row(sample_excel_file: Path, metrics: ComponentMetrics):
    r = ExcelReceiver()

    async def drain():
        out = []
        async for row in r.read_row(filepath=sample_excel_file, metrics=metrics):
            out.append(row)
        return out

    rows = await asyncio.wait_for(drain(), timeout=3.0)

    assert isinstance(rows, list)
    assert len(rows) >= 1
    assert "id" in rows[0] and "name" in rows[0]


@pytest.mark.asyncio
async def test_readexcel_bulk(sample_excel_file: Path, metrics: ComponentMetrics):
    r = ExcelReceiver()
    df = await r.read_bulk(filepath=sample_excel_file, metrics=metrics)
    assert isinstance(df, pd.DataFrame)
    assert len(df) >= 1
    assert {"id", "name"}.issubset(set(df.columns))


@pytest.mark.asyncio
async def test_readexcel_bigdata(sample_excel_file: Path, metrics: ComponentMetrics):
    r = ExcelReceiver()
    ddf = await r.read_bigdata(filepath=sample_excel_file, metrics=metrics)
    assert isinstance(ddf, dd.DataFrame)
    df = ddf.compute()
    assert len(df) >= 1
    assert {"id", "name"}.issubset(set(df.columns))


@pytest.mark.asyncio
async def test_writeexcel_row(tmp_path: Path, metrics: ComponentMetrics):
    file_path = tmp_path / "out_row.xlsx"
    r = ExcelReceiver()

    await r.write_row(
        filepath=file_path, metrics=metrics, row={"id": "10", "name": "Daisy"}
    )
    await r.write_row(
        filepath=file_path, metrics=metrics, row={"id": "11", "name": "Eli"}
    )

    df = await r.read_bulk(filepath=file_path, metrics=metrics)
    assert len(df) == 2
    assert set(df["name"]) == {"Daisy", "Eli"}


@pytest.mark.asyncio
async def test_writeexcel_bulk(tmp_path: Path, metrics: ComponentMetrics):
    file_path = tmp_path / "out_bulk.xlsx"
    file_path.touch()
    r = ExcelReceiver()

    data = [
        {"id": "20", "name": "Finn"},
        {"id": "21", "name": "Gina"},
    ]
    await r.write_bulk(filepath=file_path, metrics=metrics, data=data)

    df = await r.read_bulk(filepath=file_path, metrics=metrics)
    assert len(df) == 2
    assert set(df["name"]) == {"Finn", "Gina"}


@pytest.mark.asyncio
async def test_writeexcel_bigdata(tmp_path: Path, metrics: ComponentMetrics):
    file_path = tmp_path / "out_big.xlsx"
    file_path.touch()
    r = ExcelReceiver()

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