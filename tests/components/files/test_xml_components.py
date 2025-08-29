import asyncio
import inspect
import pytest
import pandas as pd
import dask.dataframe as dd
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import AsyncGenerator

from etl_core.components.file_components.xml.read_xml import ReadXML
from etl_core.components.file_components.xml.write_xml import WriteXML
from etl_core.strategies.row_strategy import RowExecutionStrategy
from etl_core.strategies.bulk_strategy import BulkExecutionStrategy
from etl_core.strategies.bigdata_strategy import BigDataExecutionStrategy
from etl_core.components.envelopes import Out

DATA_DIR = Path(__file__).parent.parent / "data/xml"
VALID_XML = DATA_DIR / "test_data.xml"
MISSING_VALUES_XML = DATA_DIR / "test_data_missing_values.xml"
NESTED_XML = DATA_DIR / "test_data_nested.xml"
BIGDATA_DIR = DATA_DIR / "bigdata"
INVALID_XML_FILE = DATA_DIR / "test_data_not_xml.txt"



@pytest.mark.asyncio
async def test_read_xml_valid_bulk(metrics):
    comp = ReadXML(
        name="ReadXML_Bulk_Valid",
        description="Valid XML file",
        comp_type="read_xml",
        filepath=VALID_XML,
        root_tag="rows",
        record_tag="row",
    )
    comp.strategy = BulkExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)
    assert isinstance(res, AsyncGenerator)

    async for item in res:
        assert isinstance(item, Out)
        assert item.port == "out"
        df = item.payload
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert set(df.columns) == {"id", "name"}


@pytest.mark.asyncio
async def test_read_xml_missing_values_bulk(metrics):
    comp = ReadXML(
        name="ReadXML_Bulk_Missing",
        description="Missing values",
        comp_type="read_xml",
        filepath=MISSING_VALUES_XML,
        root_tag="rows",
        record_tag="row",
    )
    comp.strategy = BulkExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)
    async for item in res:
        assert isinstance(item, Out)
        assert item.port == "out"
        df = item.payload
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert df.iloc[0]["id"] == "1"
        assert df.iloc[0]["name"] == "Alice"
        assert df.iloc[1]["id"] == "2"
        assert df.iloc[1]["name"] in ("", None, pd.NA)



@pytest.mark.asyncio
async def test_read_xml_row_streaming(metrics):
    comp = ReadXML(
        name="ReadXML_Row_Stream",
        description="Row streaming",
        comp_type="read_xml",
        filepath=VALID_XML,
        root_tag="rows",
        record_tag="row",
    )
    comp.strategy = RowExecutionStrategy()

    rows = comp.execute(payload=None, metrics=metrics)
    assert inspect.isasyncgen(rows) or isinstance(rows, AsyncGenerator)

    first = await asyncio.wait_for(anext(rows), timeout=0.25)
    assert isinstance(first, Out) and first.port == "out"
    f = first.payload
    assert set(f.keys()) == {"id", "name"}
    assert f["id"] == "1"
    assert f["name"] == "Alice"

    second = await asyncio.wait_for(anext(rows), timeout=0.25)
    assert isinstance(second, Out) and second.port == "out"
    s = second.payload
    assert set(s.keys()) == {"id", "name"}
    assert s["id"] == "2"
    assert s["name"] == "Bob"

    await rows.aclose()



@pytest.mark.asyncio
async def test_read_xml_bigdata(metrics):
    comp = ReadXML(
        name="ReadXML_BigData",
        description="Read XML with Dask",
        comp_type="read_xml",
        filepath=BIGDATA_DIR,
        root_tag="rows",
        record_tag="row",
    )
    comp.strategy = BigDataExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)
    async for item in res:
        assert isinstance(item, Out)
        assert item.port == "out"
        ddf = item.payload
        assert isinstance(ddf, dd.DataFrame)
        pdf = ddf.compute().reset_index(drop=True)
        assert len(pdf) == 3
        assert set(pdf.columns) == {"id", "name"}
        assert pdf.iloc[0]["id"] == "1"
        assert pdf.iloc[0]["name"] == "Alice"



@pytest.mark.asyncio
async def test_write_xml_row(tmp_path: Path, metrics):
    out_fp = tmp_path / "single.xml"

    comp = WriteXML(
        name="WriteXML_Row",
        description="Write single row",
        comp_type="write_xml",
        filepath=out_fp,
        root_tag="rows",
        record_tag="row",
    )
    comp.strategy = RowExecutionStrategy()

    row = {"id": "1", "name": "Zoe"}
    out = await anext(comp.execute(payload=row, metrics=metrics), None)
    assert isinstance(out, Out)
    assert out.port == "out"
    assert out.payload == row

    assert out_fp.exists()
    tree = ET.parse(out_fp)
    root = tree.getroot()
    recs = root.findall("./row")
    assert len(recs) == 1
    assert recs[0].findtext("id") == "1"
    assert recs[0].findtext("name") == "Zoe"



@pytest.mark.asyncio
async def test_write_xml_bulk(tmp_path: Path, metrics):
    out_fp = tmp_path / "bulk.xml"

    comp = WriteXML(
        name="WriteXML_Bulk",
        description="Write multiple rows",
        comp_type="write_xml",
        filepath=out_fp,
        root_tag="rows",
        record_tag="row",
    )
    comp.strategy = BulkExecutionStrategy()
    data = pd.DataFrame(
        [
            {"id": "1", "name": "A"},
            {"id": "2", "name": "B"},
            {"id": "3", "name": "C"},
        ]
    )

    out = await anext(comp.execute(payload=data, metrics=metrics), None)
    assert isinstance(out, Out)
    assert out.port == "out"
    pd.testing.assert_frame_equal(
        out.payload.reset_index(drop=True), data.reset_index(drop=True)
    )

    assert out_fp.exists()
    tree = ET.parse(out_fp)
    root = tree.getroot()
    rows = root.findall("./row")
    assert len(rows) == 3
    assert rows[0].findtext("id") == "1"
    assert rows[0].findtext("name") == "A"
    assert rows[1].findtext("id") == "2"
    assert rows[1].findtext("name") == "B"
    assert rows[2].findtext("id") == "3"
    assert rows[2].findtext("name") == "C"



@pytest.mark.asyncio
async def test_write_xml_bigdata(tmp_path: Path, metrics):
    out_dir = tmp_path / "bigxml"  # Directory expected by XML bigdata writer

    comp = WriteXML(
        name="WriteXML_BigData",
        description="Write Dask DataFrame",
        comp_type="write_xml",
        filepath=out_dir,
        root_tag="rows",
        record_tag="row",
    )
    comp.strategy = BigDataExecutionStrategy()

    ddf_in = dd.from_pandas(
        pd.DataFrame(
            [
                {"id": "10", "name": "Nina"},
                {"id": "11", "name": "Omar"},
            ]
        ),
        npartitions=2,
    )

    out = await anext(comp.execute(payload=ddf_in, metrics=metrics), None)
    assert isinstance(out, Out)
    assert out.port == "out"
    pd.testing.assert_frame_equal(
        out.payload.compute().reset_index(drop=True),
        ddf_in.compute().reset_index(drop=True),
    )

    assert out_dir.exists() and out_dir.is_dir()
    parts = sorted(out_dir.glob("part-*.xml"))
    assert len(parts) == 2

    all_ids = []
    for p in parts:
        tree = ET.parse(p)
        root = tree.getroot()
        for r in root.findall("./row"):
            all_ids.append(r.findtext("id"))
    assert set(all_ids) == {"10", "11"}



@pytest.mark.asyncio
async def test_read_xml_missing_file_bulk_raises(metrics, tmp_path: Path):
    comp = ReadXML(
        name="ReadXML_Bulk_MissingFile",
        description="missing file should error",
        comp_type="read_xml",
        filepath=tmp_path / "nope.xml",
        root_tag="rows",
        record_tag="row",
    )
    comp.strategy = BulkExecutionStrategy()

    with pytest.raises(FileNotFoundError):
        gen = comp.execute(payload=None, metrics=metrics)
        await anext(gen)


@pytest.mark.asyncio
async def test_read_xml_invalid_file_type_bulk_raises(metrics):
    comp = ReadXML(
        name="ReadXML_Bulk_InvalidFile",
        description="invalid file should error",
        comp_type="read_xml",
        filepath=INVALID_XML_FILE,
        root_tag="rows",
        record_tag="row",
    )
    comp.strategy = BulkExecutionStrategy()

    with pytest.raises(Exception):
        gen = comp.execute(payload=None, metrics=metrics)
        await anext(gen)



@pytest.mark.asyncio
async def test_read_xml_nested_bulk_flattens(metrics):
    comp = ReadXML(
        name="ReadXML_Bulk_Nested",
        description="Nested structure flattening",
        comp_type="read_xml",
        filepath=NESTED_XML,
        root_tag="rows",
        record_tag="row",
    )
    comp.strategy = BulkExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)
    async for item in res:
        df = item.payload
        assert {"id", "name", "address.street", "address.city", "tags.0", "tags.1"}.issubset(set(df.columns))
        r0 = df.iloc[0]
        assert r0["id"] == "1"
        assert r0["address.street"] == "Main"
        assert r0["tags.0"] == "alpha"
        assert r0["tags.1"] == "beta"
