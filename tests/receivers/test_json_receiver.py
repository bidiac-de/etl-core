from datetime import datetime, timedelta
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from src.receivers.files.json_receiver import JSONReceiver
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
def sample_json_file() -> Path:
    return (
        Path(__file__).parent.parent / "components" / "data" / "json" / "testdata.json"
    )


@pytest.fixture
def sample_ndjson_file() -> Path:
    return (
        Path(__file__).parent.parent / "components" / "data" / "json" / "testdata.jsonl"
    )


@pytest.mark.asyncio
async def test_readjson_row(sample_json_file: Path, metrics: ComponentMetrics):
    r = JSONReceiver()
    rows = [row async for row in r.read_row(filepath=sample_json_file, metrics=metrics)]
    assert isinstance(rows, list)
    assert len(rows) == 3
    assert {"id", "name"}.issubset(rows[0].keys())
    assert {"Alice", "Bob", "Charlie"}.issubset({x["name"] for x in rows})


@pytest.mark.asyncio
async def test_readjson_bulk(sample_json_file: Path, metrics: ComponentMetrics):
    r = JSONReceiver()
    df = await r.read_bulk(filepath=sample_json_file, metrics=metrics)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert {"id", "name"}.issubset(df.columns)
    assert "Bob" in set(df["name"])


@pytest.mark.asyncio
async def test_readjson_bigdata(sample_ndjson_file: Path, metrics: ComponentMetrics):
    r = JSONReceiver()
    ddf = await r.read_bigdata(filepath=sample_ndjson_file, metrics=metrics)
    assert isinstance(ddf, dd.DataFrame)
    df = ddf.compute()
    assert len(df) == 3
    assert "Charlie" in set(df["name"])


@pytest.mark.asyncio
async def test_writejson_row(tmp_path: Path, metrics: ComponentMetrics):
    file_path = tmp_path / "out_row.json"
    r = JSONReceiver()

    await r.write_row(
        filepath=file_path, metrics=metrics, row={"id": 10, "name": "Daisy"}
    )
    await r.write_row(
        filepath=file_path, metrics=metrics, row={"id": 11, "name": "Eli"}
    )

    df = await r.read_bulk(filepath=file_path, metrics=metrics)
    assert len(df) == 2
    assert set(df["name"]) == {"Daisy", "Eli"}


@pytest.mark.asyncio
async def test_writejson_bulk(tmp_path: Path, metrics: ComponentMetrics):
    file_path = tmp_path / "out_bulk.json"
    r = JSONReceiver()

    data = [
        {"id": 20, "name": "Finn"},
        {"id": 21, "name": "Gina"},
    ]
    await r.write_bulk(filepath=file_path, metrics=metrics, data=data)

    df = await r.read_bulk(filepath=file_path, metrics=metrics)
    assert len(df) == 2
    assert set(df["name"]) == {"Finn", "Gina"}


@pytest.mark.asyncio
async def test_writejson_bigdata(tmp_path: Path, metrics: ComponentMetrics):
    out_dir = tmp_path / "big_out"
    out_dir.mkdir(parents=True, exist_ok=True)
    r = JSONReceiver()

    pdf = pd.DataFrame(
        [
            {"id": 30, "name": "Hugo"},
            {"id": 31, "name": "Ivy"},
        ]
    )
    ddf_in = dd.from_pandas(pdf, npartitions=1)

    await r.write_bigdata(filepath=out_dir, metrics=metrics, data=ddf_in)

    parts = sorted(out_dir.glob("part-*.json"))
    assert parts, "No partition files written."

    ddf_out = dd.read_json([str(p) for p in parts], lines=True, blocksize="64MB")
    df_out = ddf_out.compute()
    assert len(df_out) == 2
    assert set(df_out["name"]) == {"Hugo", "Ivy"}


@pytest.mark.asyncio
async def test_readjson_row_gz(tmp_path: Path, metrics: ComponentMetrics):
    """
    Optional: .gz Support – only read_row (uses open_text_auto).
    """
    import gzip
    import json as _json
    import asyncio

    gz_path = tmp_path / "rows.json.gz"
    payload = [{"id": 1, "name": "Alice"}]

    def _write_gz():
        with gzip.open(gz_path, "wt", encoding="utf-8") as f:
            _json.dump(payload, f)

    await asyncio.to_thread(_write_gz)
    r = JSONReceiver()
    rows = [row async for row in r.read_row(filepath=gz_path, metrics=metrics)]
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"
