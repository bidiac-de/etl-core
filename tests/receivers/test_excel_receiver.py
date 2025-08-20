import asyncio
import inspect
from datetime import datetime, timedelta
from pathlib import Path
from typing import AsyncGenerator

import dask.dataframe as dd
import pandas as pd
import pytest

from etl_core.receivers.files.excel.excel_receiver import ExcelReceiver
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
def sample_excel_file() -> Path:
    return (
        Path(__file__).parent.parent
        / "components"
        / "files"
        / "data"
        / "test_data.xlsx"
    )


@pytest.mark.asyncio
async def test_excelreceiver_read_row_streaming(
    sample_excel_file: Path, metrics: ComponentMetrics
) -> None:
    r = ExcelReceiver()
    rows = r.read_row(filepath=sample_excel_file, metrics=metrics)

    assert inspect.isasyncgen(rows) or isinstance(rows, AsyncGenerator)

    collected = []
    for _ in range(3):
        rec = await asyncio.wait_for(anext(rows), timeout=0.5)
        collected.append(rec)

    await rows.aclose()

    assert collected[0]["name"] == "Alice"
    assert collected[1]["name"] == "Bob"
    assert collected[2]["name"] == "Charlie"
    assert metrics.lines_received == 3
    assert metrics.error_count == 0


@pytest.mark.asyncio
async def test_read_excel_bulk(
    sample_excel_file: Path, metrics: ComponentMetrics
) -> None:
    r = ExcelReceiver()
    df = await r.read_bulk(filepath=sample_excel_file, metrics=metrics)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert set(df.columns) == {"id", "name"}
    assert set(df["name"]) == {"Alice", "Bob", "Charlie"}
    assert metrics.lines_received == 3
    assert metrics.error_count == 0


@pytest.mark.asyncio
async def test_read_excel_bigdata(
    sample_excel_file: Path, metrics: ComponentMetrics
) -> None:
    r = ExcelReceiver()
    ddf = await r.read_bigdata(filepath=sample_excel_file, metrics=metrics)
    assert isinstance(ddf, dd.DataFrame)
    pdf = ddf.compute()
    assert len(pdf) == 3
    assert "Charlie" in pdf["name"].values
    assert metrics.lines_received >= 3


@pytest.mark.asyncio
async def test_write_excel_row(tmp_path: Path, metrics: ComponentMetrics) -> None:
    file_path = tmp_path / "out_row.xlsx"
    r = ExcelReceiver()

    await r.write_row(
        filepath=file_path, metrics=metrics, row={"id": "10", "name": "Daisy"}
    )
    await r.write_row(
        filepath=file_path, metrics=metrics, row={"id": "11", "name": "Eli"}
    )

    assert file_path.exists()

    df = await r.read_bulk(
        filepath=file_path,
        metrics=ComponentMetrics(
            started_at=datetime.now(),
            processing_time=timedelta(0),
            error_count=0,
            lines_received=0,
            lines_forwarded=0,
        ),
    )
    assert len(df) == 2
    assert set(df["name"]) == {"Daisy", "Eli"}


@pytest.mark.asyncio
async def test_write_excel_bulk(tmp_path: Path, metrics: ComponentMetrics) -> None:
    file_path = tmp_path / "out_bulk.xlsx"
    r = ExcelReceiver()

    data = pd.DataFrame(
        [
            {"id": "20", "name": "Finn"},
            {"id": "21", "name": "Gina"},
        ]
    )

    await r.write_bulk(filepath=file_path, metrics=metrics, data=data)

    df = await r.read_bulk(
        filepath=file_path,
        metrics=ComponentMetrics(
            started_at=datetime.now(),
            processing_time=timedelta(0),
            error_count=0,
            lines_received=0,
            lines_forwarded=0,
        ),
    )
    assert len(df) == 2
    assert set(df["name"]) == {"Finn", "Gina"}


@pytest.mark.asyncio
async def test_write_excel_bigdata(tmp_path: Path, metrics: ComponentMetrics) -> None:
    file_path = tmp_path / "out_big.xlsx"
    r = ExcelReceiver()

    pdf = pd.DataFrame(
        [
            {"id": 30, "name": "Hugo"},
            {"id": 31, "name": "Ivy"},
        ]
    )
    ddf_in = dd.from_pandas(pdf, npartitions=1)

    await r.write_bigdata(filepath=file_path, metrics=metrics, data=ddf_in)

    df_out = await r.read_bulk(
        filepath=file_path,
        metrics=ComponentMetrics(
            started_at=datetime.now(),
            processing_time=timedelta(0),
            error_count=0,
            lines_received=0,
            lines_forwarded=0,
        ),
    )
    assert len(df_out) == 2
    assert set(df_out["name"]) == {"Hugo", "Ivy"}


@pytest.mark.asyncio
async def test_excelreceiver_missing_file_bulk_raises(
    metrics: ComponentMetrics, tmp_path: Path
):
    r = ExcelReceiver()
    with pytest.raises(FileNotFoundError):
        await r.read_bulk(filepath=tmp_path / "missing.xlsx", metrics=metrics)


@pytest.mark.asyncio
async def test_excelreceiver_invalid_file_raises(
    metrics: ComponentMetrics, tmp_path: Path
):
    invalid = tmp_path / "invalid.xlsx"
    invalid.write_text("not an excel file")
    r = ExcelReceiver()
    with pytest.raises(Exception):
        await r.read_bulk(filepath=invalid, metrics=metrics)
