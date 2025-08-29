import pytest
import pandas as pd
import dask.dataframe as dd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Literal, AsyncGenerator

from src.strategies.row_strategy import RowExecutionStrategy
from src.strategies.bulk_strategy import BulkExecutionStrategy
from src.strategies.bigdata_strategy import BigDataExecutionStrategy

from src.components.file_components.xml.read_xml import ReadXML
from src.components.file_components.xml.write_xml import WriteXML

from src.components.column_definition import ColumnDefinition, DataType
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.components import Schema


DATA_DIR = Path(__file__).parent / "data" / "xml"
VALID_XML = DATA_DIR / "test_data.xml"
EXTRA_MISSING_XML = DATA_DIR / "testdata_extra_missing.xml"
MIXED_TYPES_XML = DATA_DIR / "testdata_mixed_types.xml"
NESTED_LIKE_XML = DATA_DIR / "testdata_nested_like.xml"
INVALID_XML_FILE = DATA_DIR / "testdata_bad.xml"
BIG_XML = DATA_DIR / "testdata_big.xml"


def build_minimal_schema() -> Schema:
    return Schema(
        columns=[
            ColumnDefinition(name="id", data_type=DataType.STRING),
            ColumnDefinition(name="name", data_type=DataType.STRING),
        ]
    )


Mode = Literal["row", "bulk", "bigdata"]


async def _consume_async_gen(gen: AsyncGenerator):
    items = []
    async for item in gen:
        items.append(item)
    return items


async def _coerce_async_result(res: Any, *, mode: Mode, component_type: str):
    """
    Standardizes behavior to match the JSON tests:
    - Row: for write_* with exactly 1 item → return that item; otherwise return a list
    - Bulk/Bigdata: if there's a single chunk, return it as a standalone object (df/ddf)
    """
    if hasattr(res, "__aiter__"):
        items = await _consume_async_gen(res)
        if mode == "row":
            return (
                items[0]
                if (component_type == "write_xml" and len(items) == 1)
                else items
            )
        return items[0] if items else None

    if hasattr(res, "__await__"):
        return await res

    return res


@pytest.fixture(autouse=True)
def patch_strategies(monkeypatch):
    from src.strategies.row_strategy import RowExecutionStrategy
    from src.strategies.bulk_strategy import BulkExecutionStrategy
    from src.strategies.bigdata_strategy import BigDataExecutionStrategy

    async def row_exec(self, component, payload, metrics):
        res = component.process_row(payload, metrics=metrics)
        return await _coerce_async_result(
            res, mode="row", component_type=getattr(component, "type", "")
        )

    async def bulk_exec(self, component, payload, metrics):
        res = component.process_bulk(payload, metrics=metrics)
        return await _coerce_async_result(
            res, mode="bulk", component_type=getattr(component, "type", "")
        )

    async def bigdata_exec(self, component, payload, metrics):
        res = component.process_bigdata(payload, metrics=metrics)
        return await _coerce_async_result(
            res, mode="bigdata", component_type=getattr(component, "type", "")
        )

    monkeypatch.setattr(RowExecutionStrategy, "execute", row_exec, raising=True)
    monkeypatch.setattr(BulkExecutionStrategy, "execute", bulk_exec, raising=True)
    monkeypatch.setattr(BigDataExecutionStrategy, "execute", bigdata_exec, raising=True)


@pytest.fixture
def schema_definition():
    return [
        ColumnDefinition(name="id", data_type=DataType.STRING),
        ColumnDefinition(name="name", data_type=DataType.STRING),
    ]


@pytest.fixture
def metrics() -> ComponentMetrics:
    return ComponentMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
    )


@pytest.mark.asyncio
async def test_readxml_valid_bulk(schema_definition, metrics):
    comp = ReadXML(
        name="ReadXML_Bulk_Valid",
        description="Valid XML rows",
        comp_type="read_xml",
        filepath=VALID_XML,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    comp.strategy = BulkExecutionStrategy()

    df = await comp.execute(payload=None, metrics=metrics)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert set(df.columns) >= {"id", "name"}
    assert set(df["name"]) == {"Alice", "Bob", "Charlie"}


@pytest.mark.asyncio
async def test_readxml_invalid_content_raises(schema_definition, metrics):
    comp = ReadXML(
        name="ReadXML_Invalid",
        description="Malformed XML",
        comp_type="read_xml",
        filepath=INVALID_XML_FILE,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    comp.strategy = BulkExecutionStrategy()

    with pytest.raises(Exception):
        _ = await comp.execute(payload=None, metrics=metrics)


@pytest.mark.asyncio
async def test_readxml_bulk_extra_missing(schema_definition, metrics):
    comp = ReadXML(
        name="ReadXML_ExtraMissing",
        description="Extra + missing fields",
        comp_type="read_xml",
        filepath=EXTRA_MISSING_XML,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    comp.strategy = BulkExecutionStrategy()

    df = await comp.execute(payload=None, metrics=metrics)
    assert isinstance(df, pd.DataFrame)
    assert {"id", "name"}.issubset(df.columns)
    assert {"city", "age"}.issubset(df.columns)
    assert df.isna().any().any()


@pytest.mark.asyncio
async def test_readxml_bulk_mixed_types(schema_definition, metrics):
    comp = ReadXML(
        name="ReadXML_MixedTypes",
        description="Mixed numeric/string/empty",
        comp_type="read_xml",
        filepath=MIXED_TYPES_XML,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    comp.strategy = BulkExecutionStrategy()

    df = await comp.execute(payload=None, metrics=metrics)
    assert "score" in df.columns
    pd.to_numeric(df["score"], errors="coerce")


@pytest.mark.asyncio
async def test_readxml_bulk_nested_is_flat_by_default(schema_definition, metrics):
    comp = ReadXML(
        name="ReadXML_Nested",
        description="Keep nested info as raw text",
        comp_type="read_xml",
        filepath=NESTED_LIKE_XML,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    comp.strategy = BulkExecutionStrategy()

    df = await comp.execute(payload=None, metrics=metrics)
    assert "addr" in df.columns
    assert isinstance(df.iloc[0]["addr"], str)


@pytest.mark.asyncio
async def test_readxml_row_streaming(schema_definition, metrics):
    comp = ReadXML(
        name="ReadXML_Row_Stream",
        description="Row streaming over XML",
        comp_type="read_xml",
        filepath=VALID_XML,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    comp.strategy = RowExecutionStrategy()

    rows: List[Dict[str, Any]] = await comp.execute(payload=None, metrics=metrics)
    assert isinstance(rows, list)
    assert len(rows) == 3
    assert set(rows[0].keys()) >= {"id", "name"}


@pytest.mark.asyncio
async def test_readxml_bigdata(schema_definition, metrics):
    comp = ReadXML(
        name="ReadXML_BigData",
        description="Read XML via Dask wrapper",
        comp_type="read_xml",
        filepath=BIG_XML,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    comp.strategy = BigDataExecutionStrategy()

    ddf = await comp.execute(payload=None, metrics=metrics)
    assert isinstance(ddf, dd.DataFrame)
    df = ddf.compute()
    assert len(df) == 5
    assert {"User1", "User5"}.issubset(set(df["name"]))


@pytest.mark.asyncio
async def test_writexml_row(tmp_path: Path, schema_definition, metrics):
    out_fp = tmp_path / "single.xml"

    comp = WriteXML(
        name="WriteXML_Row",
        description="Write single row",
        comp_type="write_xml",
        filepath=out_fp,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    comp.strategy = RowExecutionStrategy()

    row = {"id": "1", "name": "Zoe"}
    result = await comp.execute(payload=row, metrics=metrics)
    assert result == row
    assert out_fp.exists()

    reader = ReadXML(
        name="ReadBack_Row_XML",
        description="Read back single",
        comp_type="read_xml",
        filepath=out_fp,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    reader.strategy = BulkExecutionStrategy()
    df = await reader.execute(payload=None, metrics=metrics)
    assert len(df) == 1 and df.iloc[0]["name"] == "Zoe"


@pytest.mark.asyncio
async def test_writexml_bulk(tmp_path: Path, schema_definition, metrics):
    out_fp = tmp_path / "bulk.xml"

    comp = WriteXML(
        name="WriteXML_Bulk",
        description="Write list of records",
        comp_type="write_xml",
        filepath=out_fp,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    comp.strategy = BulkExecutionStrategy()

    data = [
        {"id": "1", "name": "A"},
        {"id": "2", "name": "B"},
        {"id": "3", "name": "C"},
    ]
    res = await comp.execute(payload=data, metrics=metrics)
    assert isinstance(res, list) and len(res) == 3
    assert out_fp.exists()

    reader = ReadXML(
        name="ReadBack_Bulk_XML",
        description="Read back bulk XML",
        comp_type="read_xml",
        filepath=out_fp,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
    )
    reader.strategy = BulkExecutionStrategy()
    df = await reader.execute(payload=None, metrics=metrics)
    assert list(df.sort_values("id")["name"]) == ["A", "B", "C"]


@pytest.mark.asyncio
async def test_writexml_bigdata(tmp_path: Path, schema_definition, metrics):
    out_dir = tmp_path / "big_out"
    out_dir.mkdir(parents=True, exist_ok=True)

    comp = WriteXML(
        name="WriteXML_BigData",
        description="Write partitioned XML",
        comp_type="write_xml",
        filepath=out_dir,
        schema_definition=schema_definition,
        schema=build_minimal_schema(),
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

    result = await comp.execute(payload=ddf_in, metrics=metrics)
    assert isinstance(result, dd.DataFrame)

    parts = sorted(out_dir.glob("part-*.xml"))
    assert parts, "No partition files written."

    pdfs = []
    for p in parts:
        reader = ReadXML(
            name=f"ReadBack_{p.name}",
            description="Read partition",
            comp_type="read_xml",
            filepath=p,
            schema_definition=schema_definition,
            schema=build_minimal_schema(),
        )
        reader.strategy = BulkExecutionStrategy()
        pdfs.append(await reader.execute(payload=None, metrics=metrics))
    df_out = pd.concat(pdfs, ignore_index=True).sort_values("id")
    assert list(df_out["name"]) == ["Nina", "Omar"]