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
    return (
        Path(__file__).parent.parent.parent
        / "components"
        / "data"
        / "xml"
        / "test_data.xml"
    )


@pytest.fixture
def sample_bigdata_file() -> Path:
    return (
        Path(__file__).parent.parent.parent
        / "components"
        / "data"
        / "xml"
        / "test_bigdata.xml"
    )


@pytest.mark.asyncio
async def test_xml_receiver_read_row_streaming(
    sample_xml_file: Path, metrics: ComponentMetrics
):
    r = XMLReceiver()

    rows = r.read_row(
        filepath=sample_xml_file,
        metrics=metrics,
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
    dfs = []
    async for df in r.read_bulk(
        filepath=sample_xml_file, metrics=metrics, record_tag="row"
    ):
        assert isinstance(df, pd.DataFrame)
        dfs.append(df)
    all_df = pd.concat(dfs, ignore_index=True)

    assert len(all_df) == 3
    assert {"id", "name"}.issubset(set(all_df.columns))
    assert "Bob" in set(all_df["name"])


@pytest.mark.asyncio
async def test_read_xml_bigdata(sample_bigdata_file: Path, metrics: ComponentMetrics):
    r = XMLReceiver()
    dfs = []
    async for df in r.read_bigdata(
        filepath=sample_bigdata_file, metrics=metrics, record_tag="row"
    ):
        assert isinstance(df, dd.DataFrame)
        pdf = df.compute()
        dfs.append(pdf)
    all_df = pd.concat(dfs, ignore_index=True)

    assert len(all_df) == 3
    assert {"id", "name"}.issubset(set(all_df.columns))
    assert list(all_df["id"]) == ["1", "2", "3"]


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
    out_fp = tmp_path / "out_big.xml"
    r = XMLReceiver()

    pdf = pd.DataFrame([{"id": "30", "name": "Hugo"}, {"id": "31", "name": "Ivy"}])
    ddf_in = dd.from_pandas(pdf, npartitions=2)

    await r.write_bigdata(
        filepath=out_fp,
        metrics=metrics,
        data=ddf_in,
        root_tag="rows",
        record_tag="row",
    )

    assert out_fp.exists()
    tree = ET.parse(out_fp)
    rows = tree.getroot().findall("./row")
    got = [(x.findtext("id"), x.findtext("name")) for x in rows]
    assert set(got) == {("30", "Hugo"), ("31", "Ivy")}


@pytest.mark.asyncio
async def test_xml_receiver_read_row_missing_file_raises(
    metrics: ComponentMetrics, tmp_path: Path
):
    r = XMLReceiver()
    rows = r.read_row(
        filepath=tmp_path / "missing.xml",
        metrics=metrics,
        record_tag="row",
    )
    with pytest.raises(FileNotFoundError):
        await anext(rows)
