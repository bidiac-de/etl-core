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
BIGDATA_FILE = DATA_DIR / "test_bigdata.xml"
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

    chunks = []
    async for item in res:
        assert isinstance(item, Out) and item.port == "out"
        assert isinstance(item.payload, pd.DataFrame)
        chunks.append(item.payload)

    assert chunks
    df_all = pd.concat(chunks, ignore_index=True)
    assert {"id", "name"}.issubset(set(df_all.columns))
    assert len(df_all) == 3

    assert metrics.lines_forwarded == 3
    assert metrics.lines_received == 0



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

    chunks = []
    async for item in res:
        chunks.append(item.payload)

    df_all = pd.concat(chunks, ignore_index=True)
    assert len(df_all) == 3
    assert df_all.iloc[0]["id"] == "1"
    assert df_all.iloc[0]["name"] == "Alice"
    assert df_all.iloc[1]["id"] == "2"
    assert df_all.iloc[1]["name"] in ("", None, pd.NA)

    assert metrics.lines_forwarded == 3
    assert metrics.lines_received == 0




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

    async for _ in rows:
        pass

    assert metrics.lines_forwarded == 3
    assert metrics.lines_received == 0

    await rows.aclose()



@pytest.mark.asyncio
async def test_read_xml_bigdata(metrics):
    comp = ReadXML(
        name="ReadXML_BigData",
        description="Read XML chunked",
        comp_type="read_xml",
        filepath=BIGDATA_FILE,
        root_tag="rows",
        record_tag="row",
    )
    comp.strategy = BigDataExecutionStrategy()

    chunks = []
    async for item in comp.execute(payload=None, metrics=metrics):
        assert isinstance(item, Out) and isinstance(item.payload, pd.DataFrame)
        chunks.append(item.payload)

    df_all = pd.concat(chunks, ignore_index=True)
    assert {"id", "name"}.issubset(set(df_all.columns))
    assert len(df_all) == 3
    assert df_all.iloc[0]["id"] == "1"
    assert df_all.iloc[0]["name"] == "Alice"

    assert metrics.lines_forwarded == 3
    assert metrics.lines_received == 0




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

    assert metrics.lines_received == 1
    assert metrics.lines_forwarded == 1



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

    assert metrics.lines_received == 3
    assert metrics.lines_forwarded == 3




@pytest.mark.asyncio
async def test_write_xml_bigdata(tmp_path: Path, metrics):
    out_fp = tmp_path / "test_bigdata.xml"

    comp = WriteXML(
        name="WriteXML_BigData",
        description="Write big data like bulk (single file)",
        comp_type="write_xml",
        filepath=out_fp,
        root_tag="rows",
        record_tag="row",
    )
    comp.strategy = BigDataExecutionStrategy()

    ddf_in = dd.from_pandas(pd.DataFrame(
        [{"id": "10", "name": "Nina"}, {"id": "11", "name": "Omar"}]
    ), npartitions=2)

    out = await anext(comp.execute(payload=ddf_in, metrics=metrics), None)
    assert isinstance(out, Out) and out.port == "out"

    assert out_fp.exists()
    tree = ET.parse(out_fp)
    rows = tree.getroot().findall("./row")
    got = [(r.findtext("id"), r.findtext("name")) for r in rows]
    assert set(got) == {("10", "Nina"), ("11", "Omar")}

    assert metrics.lines_received == 2
    assert metrics.lines_forwarded == 2



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
    chunks = []
    async for item in comp.execute(payload=None, metrics=metrics):
        chunks.append(item.payload)
    df_all = pd.concat(chunks, ignore_index=True)
    assert {"id","name","address.street","address.city","tags.item[0]","tags.item[1]"} <= set(df_all.columns)

    r0 = df_all.iloc[0]
    assert r0["id"] == "1"
    assert r0["address.street"] == "Main"
    assert r0["tags.item[0]"] == "alpha"
    assert r0["tags.item[1]"] == "beta"

    assert metrics.lines_forwarded == 2
    assert metrics.lines_received == 0


@pytest.mark.asyncio
async def test_write_xml_row_from_flat_dict(tmp_path: Path, metrics):
    out_fp = tmp_path / "row_flat.xml"
    comp = WriteXML(
        name="WriteXML_Row_Flat",
        description="row from flat keys",
        comp_type="write_xml",
        filepath=out_fp,
        root_tag="rows",
        record_tag="row",
    )
    comp.strategy = RowExecutionStrategy()

    row = {
        "id": "1",
        "name": "Alice",
        "address.street": "Main",
        "address.city": "Springfield",
        "address.@attrs.cityId": "X-42",
        "note.#text": "hello",
        "tags.item[0]": "alpha",
        "tags.item[1]": "beta",
    }

    out = await anext(comp.execute(payload=row, metrics=metrics), None)
    assert isinstance(out, Out) and out.port == "out"

    tree = ET.parse(out_fp)
    root = tree.getroot()
    rec = root.find("./row")
    assert rec is not None
    assert rec.findtext("id") == "1"
    assert rec.findtext("name") == "Alice"

    addr = rec.find("./address")
    assert addr is not None
    assert addr.get("cityId") == "X-42"
    assert addr.findtext("street") == "Main"
    assert addr.findtext("city") == "Springfield"

    note = rec.find("./note")
    assert note is not None and (note.text or "") == "hello"

    items = rec.findall("./tags/item")
    assert [i.text for i in items] == ["alpha", "beta"]

    assert metrics.lines_received == 1
    assert metrics.lines_forwarded == 1


@pytest.mark.asyncio
async def test_write_xml_bulk_from_flat_df(tmp_path: Path, metrics):
    out_fp = tmp_path / "bulk_flat.xml"
    comp = WriteXML(
        name="WriteXML_Bulk_Flat",
        description="bulk from flat df",
        comp_type="write_xml",
        filepath=out_fp,
        root_tag="rows",
        record_tag="row",
    )
    comp.strategy = BulkExecutionStrategy()

    df = pd.DataFrame([
        {
            "id": "1", "name": "A",
            "address.street": "Main", "address.city": "Town",
            "tags.item[0]": "t1", "tags.item[1]": "t2",
        },
        {
            "id": "2", "name": "B",
            "address.street": "Second", "address.city": "Ville",
            "tags.item[0]": "u1",
        },
    ])

    out = await anext(comp.execute(payload=df, metrics=metrics), None)
    assert isinstance(out, Out) and out.port == "out"

    tree = ET.parse(out_fp)
    rows = tree.getroot().findall("./row")
    assert len(rows) == 2

    r1, r2 = rows
    assert r1.findtext("id") == "1"
    assert r1.findtext("address/street") == "Main"
    assert r1.findtext("address/city") == "Town"
    assert [e.text for e in r1.findall("./tags/item")] == ["t1", "t2"]

    assert r2.findtext("id") == "2"
    assert r2.findtext("address/street") == "Second"
    assert r2.findtext("address/city") == "Ville"
    assert [e.text for e in r2.findall("./tags/item")] == ["u1"]

    assert metrics.lines_received == 2
    assert metrics.lines_forwarded == 2



