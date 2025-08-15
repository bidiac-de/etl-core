from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import dask.dataframe as dd
import pytest

from src.receivers.files.xml_receiver import XMLReceiver
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
def sample_xml_file() -> Path:
    return Path(__file__).parent.parent / "components" / "data" / "xml" / "testdata.xml"


@pytest.fixture
def sample_big_xml_file() -> Path:
    return Path(__file__).parent.parent / "components" / "data" / "xml" / "testdata_big.xml"


@pytest.mark.asyncio
async def test_readxml_row(sample_xml_file: Path, metrics: ComponentMetrics):
    r = XMLReceiver()
    rows = []
# root_tag/record_tag are optional; defaults are "rows"/"row"
    async for rec in r.read_row(filepath=sample_xml_file, metrics=metrics):
        rows.append(rec)
    assert isinstance(rows, list)
    assert len(rows) == 3
    assert {"id", "name"}.issubset(rows[0].keys())
    assert {"Alice", "Bob", "Charlie"}.issubset({x["name"] for x in rows})


@pytest.mark.asyncio
async def test_readxml_bulk(sample_xml_file: Path, metrics: ComponentMetrics):
    r = XMLReceiver()
    df = await r.read_bulk(filepath=sample_xml_file, metrics=metrics)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert {"id", "name"}.issubset(df.columns)
    assert "Bob" in set(df["name"])


@pytest.mark.asyncio
async def test_readxml_bigdata(sample_big_xml_file: Path, metrics: ComponentMetrics):
    r = XMLReceiver()
    ddf = await r.read_bigdata(filepath=sample_big_xml_file, metrics=metrics)
    assert isinstance(ddf, dd.DataFrame)
    df = ddf.compute()
    assert len(df) == 5
    assert {"User1", "User5"}.issubset(set(df["name"]))


@pytest.mark.asyncio
async def test_writexml_row(tmp_path: Path, metrics: ComponentMetrics):
    file_path = tmp_path / "out_row.xml"
    r = XMLReceiver()

    await r.write_row(
        filepath=file_path,
        metrics=metrics,
        row={"id": 10, "name": "Daisy"},
    )
    await r.write_row(
        filepath=file_path,
        metrics=metrics,
        row={"id": 11, "name": "Eli"},
    )

    df = await r.read_bulk(filepath=file_path, metrics=metrics)
    assert len(df) == 2
    assert set(df["name"]) == {"Daisy", "Eli"}


@pytest.mark.asyncio
async def test_writexml_bulk(tmp_path: Path, metrics: ComponentMetrics):
    file_path = tmp_path / "out_bulk.xml"
    r = XMLReceiver()

    data = [
        {"id": 20, "name": "Finn"},
        {"id": 21, "name": "Gina"},
    ]
    await r.write_bulk(filepath=file_path, metrics=metrics, data=data)

    df = await r.read_bulk(filepath=file_path, metrics=metrics)
    assert len(df) == 2
    assert set(df["name"]) == {"Finn", "Gina"}


@pytest.mark.asyncio
async def test_writexml_bigdata(tmp_path: Path, metrics: ComponentMetrics):
    out_dir = tmp_path / "big_out"
    out_dir.mkdir(parents=True, exist_ok=True)
    r = XMLReceiver()

    pdf = pd.DataFrame(
        [
            {"id": 30, "name": "Hugo"},
            {"id": 31, "name": "Ivy"},
        ]
    )
    ddf_in = dd.from_pandas(pdf, npartitions=2)

    await r.write_bigdata(filepath=out_dir, metrics=metrics, data=ddf_in)

    parts = sorted(out_dir.glob("part-*.xml"))
    assert parts, "No partition files written."

    pdfs = []
    for p in parts:
        df_part = await r.read_bulk(filepath=p, metrics=metrics)
        pdfs.append(df_part)
    df_out = pd.concat(pdfs, ignore_index=True)
    assert len(df_out) == 2
    assert set(df_out["name"]) == {"Hugo", "Ivy"}
