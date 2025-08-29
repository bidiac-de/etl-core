import asyncio
import inspect
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from typing import AsyncGenerator

import dask.dataframe as dd
import pandas as pd
import pytest

from etl_core.receivers.files.xml.xml_receiver import XMLReceiver
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
def sample_xml_file() -> Path:
    return Path(__file__).parent.parent / "components" / "data" / "xml" / "test_data.xml"


@pytest.fixture
def sample_bigdata_dir() -> Path:
    return Path(__file__).parent.parent / "components" / "data" / "xml" / "bigdata"



@pytest.mark.asyncio
async def test_xml_receiver_read_row_streaming(sample_xml_file: Path, metrics: ComponentMetrics):
    r = XMLReceiver()

    rows = r.read_row(
        filepath=sample_xml_file,
        metrics=metrics,
        root_tag="rows",
        record_tag="row",
    )

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
async def test_read_xml_bulk(sample_xml_file: Path, metrics: ComponentMetrics):
    r = XMLReceiver()
    df = await r.read_bulk(
        filepath=sample_xml_file, metrics=metrics, root_tag="rows", record_tag="row"
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert set(df.columns) == {"id", "name"}
    assert "Bob" in set(df["name"])



@pytest.mark.asyncio
async def test_read_xml_bigdata(sample_bigdata_dir: Path, metrics: ComponentMetrics):
    r = XMLReceiver()
    ddf = await r.read_bigdata(
        filepath=sample_bigdata_dir, metrics=metrics, root_tag="rows", record_tag="row"
    )
    assert isinstance(ddf, dd.DataFrame)
    pdf = ddf.compute().reset_index(drop=True)
    assert len(pdf) == 3
    assert set(pdf.columns) == {"id", "name"}
    assert list(pdf["id"]) == ["1", "2", "3"]



@pytest.mark.asyncio
async def test_write_xml_row(tmp_path: Path, metrics: ComponentMetrics):
    file_path = tmp_path / "out_row.xml"
    r = XMLReceiver()

    await r.write_row(
        filepath=file_path,
        metrics=metrics,
        row={"id": "10", "name": "Daisy"},
        root_tag="rows",
        record_tag="row",
    )

    assert file_path.exists()
    tree = ET.parse(file_path)
    root = tree.getroot()
    rows = root.findall("./row")
    assert len(rows) == 1
    assert rows[0].findtext("id") == "10"
    assert rows[0].findtext("name") == "Daisy"

    # append another row
    await r.write_row(
        filepath=file_path,
        metrics=metrics,
        row={"id": "11", "name": "Eli"},
        root_tag="rows",
        record_tag="row",
    )

    tree = ET.parse(file_path)
    root = tree.getroot()
    rows = root.findall("./row")
    assert len(rows) == 2
    assert rows[1].findtext("id") == "11"
    assert rows[1].findtext("name") == "Eli"



@pytest.mark.asyncio
async def test_write_xml_bulk(tmp_path: Path, metrics: ComponentMetrics):
    file_path = tmp_path / "out_bulk.xml"
    r = XMLReceiver()

    df_in = pd.DataFrame(
        [
            {"id": "20", "name": "Finn"},
            {"id": "21", "name": "Gina"},
        ]
    )

    await r.write_bulk(
        filepath=file_path,
        metrics=metrics,
        data=df_in,
        root_tag="rows",
        record_tag="row",
    )

    assert file_path.exists()
    # parse & compare
    tree = ET.parse(file_path)
    root = tree.getroot()
    rows = root.findall("./row")
    assert len(rows) == 2
    assert rows[0].findtext("id") == "20"
    assert rows[0].findtext("name") == "Finn"
    assert rows[1].findtext("id") == "21"
    assert rows[1].findtext("name") == "Gina"



@pytest.mark.asyncio
async def test_write_xml_bigdata(tmp_path: Path, metrics: ComponentMetrics):
    out_dir = tmp_path / "out_bigxml"
    r = XMLReceiver()

    pdf = pd.DataFrame(
        [
            {"id": "30", "name": "Hugo"},
            {"id": "31", "name": "Ivy"},
        ]
    )
    ddf_in = dd.from_pandas(pdf, npartitions=2)

    await r.write_bigdata(
        filepath=out_dir,
        metrics=metrics,
        data=ddf_in,
        root_tag="rows",
        record_tag="row",
    )

    assert out_dir.exists() and out_dir.is_dir()
    parts = sorted(out_dir.glob("part-*.xml"))
    # with 2 partitions we expect 2 files
    assert len(parts) == 2

    # Collect IDs written across parts
    ids = []
    for p in parts:
        tree = ET.parse(p)
        root = tree.getroot()
        for rnode in root.findall("./row"):
            ids.append(rnode.findtext("id"))

    assert set(ids) == {"30", "31"}



@pytest.mark.asyncio
async def test_xml_receiver_read_row_missing_file_raises(metrics: ComponentMetrics, tmp_path: Path):
    r = XMLReceiver()
    rows = r.read_row(
        filepath=tmp_path / "missing.xml",
        metrics=metrics,
        root_tag="rows",
        record_tag="row",
    )
    with pytest.raises(FileNotFoundError):
        await anext(rows)
