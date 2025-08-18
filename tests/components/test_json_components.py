import json
import pytest
import pandas as pd
import dask.dataframe as dd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Literal, AsyncGenerator

from src.strategies.row_strategy import RowExecutionStrategy
from src.strategies.bulk_strategy import BulkExecutionStrategy
from src.strategies.bigdata_strategy import BigDataExecutionStrategy

from src.components.file_components.json.read_json_component import ReadJSON
from src.components.file_components.json.write_json_component import WriteJSON

from src.components.column_definition import ColumnDefinition, DataType
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.components import Schema


DATA_DIR = Path(__file__).parent / "data" / "json"
VALID_JSON = DATA_DIR / "testdata.json"
VALID_NDJSON = DATA_DIR / "testdata.jsonl"
EXTRA_MISSING_JSON = DATA_DIR / "testdata_extra_missing.json"
MIXED_TYPES_JSON = DATA_DIR / "testdata_mixed_types.json"
NESTED_JSON = DATA_DIR / "testdata_nested.json"
INVALID_JSON_FILE = DATA_DIR / "testdata_bad.json"
BAD_LINE_JSONL = DATA_DIR / "testdata_bad_line.jsonl"
MIXED_SCHEMA_JSONL = DATA_DIR / "testdata_mixed_schema.jsonl"


Mode = Literal["row", "bulk", "bigdata"]


async def _consume_async_gen(gen: AsyncGenerator):
    items = []
    async for item in gen:
        items.append(item)
    return items


async def _coerce_async_result(res: Any, *, mode: Mode, component_type: str):
    if hasattr(res, "__aiter__"):
        items = await _consume_async_gen(res)
        if mode == "row":
            return (
                items[0]
                if (component_type == "write_json" and len(items) == 1)
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
    return Schema(
        columns=[
            ColumnDefinition(name="id", data_type=DataType.STRING),
            ColumnDefinition(name="name", data_type=DataType.STRING),
        ]
    )


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
async def test_readjson_valid_bulk(schema_definition, metrics):
    comp = ReadJSON(
        name="ReadJSON_Bulk_Valid",
        description="Valid JSON array-of-records",
        comp_type="read_json",
        filepath=VALID_JSON,
        schema=schema_definition
    )
    comp.strategy = BulkExecutionStrategy()

    df = await comp.execute(payload=None, metrics=metrics)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert set(df.columns) >= {"id", "name"}


@pytest.mark.asyncio
async def test_readjson_ndjson_bulk(schema_definition, metrics):
    comp = ReadJSON(
        name="ReadJSON_Bulk_NDJSON",
        description="Valid NDJSON",
        comp_type="read_json",
        filepath=VALID_NDJSON,
        schema=schema_definition
    )
    comp.strategy = BulkExecutionStrategy()

    df = await comp.execute(payload=None, metrics=metrics)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert set(df.columns) >= {"id", "name"}


@pytest.mark.asyncio
async def test_readjson_invalid_json_content_raises(schema_definition, metrics):
    comp = ReadJSON(
        name="ReadJSON_Invalid_Content",
        description="Malformed JSON",
        comp_type="read_json",
        filepath=INVALID_JSON_FILE,
        schema=schema_definition
    )
    comp.strategy = BulkExecutionStrategy()

    with pytest.raises(Exception):
        _ = await comp.execute(payload=None, metrics=metrics)


@pytest.mark.asyncio
async def test_readjson_bulk_extra_missing(schema_definition, metrics):
    comp = ReadJSON(
        name="ReadJSON_ExtraMissing",
        description="Extra + missing fields",
        comp_type="read_json",
        filepath=EXTRA_MISSING_JSON,
        schema=schema_definition
    )
    comp.strategy = BulkExecutionStrategy()

    df = await comp.execute(payload=None, metrics=metrics)
    assert isinstance(df, pd.DataFrame)
    # Union der Spalten + NaNs erlaubt
    assert {"id", "name"}.issubset(df.columns)
    assert {"age", "city", "nickname"}.issubset(df.columns)


@pytest.mark.asyncio
async def test_readjson_bulk_mixed_types(schema_definition, metrics):
    comp = ReadJSON(
        name="ReadJSON_MixedTypes",
        description="Mixed numeric/string/null",
        comp_type="read_json",
        filepath=MIXED_TYPES_JSON,
        schema=schema_definition
    )
    comp.strategy = BulkExecutionStrategy()

    df = await comp.execute(payload=None, metrics=metrics)
    assert "score" in df.columns
    # Keine Exception bei mixed types
    pd.to_numeric(df["score"], errors="coerce")


@pytest.mark.asyncio
async def test_readjson_bulk_nested(schema_definition, metrics):
    comp = ReadJSON(
        name="ReadJSON_Nested",
        description="Keep nested dicts",
        comp_type="read_json",
        filepath=NESTED_JSON,
        schema=schema_definition
    )
    comp.strategy = BulkExecutionStrategy()

    df = await comp.execute(payload=None, metrics=metrics)
    assert "addr" in df.columns
    assert isinstance(df.iloc[0]["addr"], dict) or df.iloc[0]["addr"] is None


@pytest.mark.asyncio
async def test_readjson_ndjson_bad_line_raises(schema_definition, metrics):
    comp = ReadJSON(
        name="ReadJSON_NDJSON_BadLine",
        description="Bad line in NDJSON",
        comp_type="read_json",
        filepath=BAD_LINE_JSONL,
        schema=schema_definition
    )
    comp.strategy = BulkExecutionStrategy()

    with pytest.raises(Exception):
        _ = await comp.execute(payload=None, metrics=metrics)


@pytest.mark.asyncio
async def test_readjson_ndjson_mixed_schema(schema_definition, metrics):
    comp = ReadJSON(
        name="ReadJSON_NDJSON_MixedSchema",
        description="Varying schema in NDJSON",
        comp_type="read_json",
        filepath=MIXED_SCHEMA_JSONL,
        schema=schema_definition
    )
    comp.strategy = BulkExecutionStrategy()

    df = await comp.execute(payload=None, metrics=metrics)
    assert set(df.columns) >= {"id", "name", "nickname", "active"}


@pytest.mark.asyncio
async def test_readjson_row_streaming(schema_definition, metrics):
    comp = ReadJSON(
        name="ReadJSON_Row_Stream",
        description="Row streaming over JSON array",
        comp_type="read_json",
        filepath=VALID_JSON,
        schema=schema_definition
    )
    comp.strategy = RowExecutionStrategy()

    rows = await comp.execute(payload=None, metrics=metrics)
    assert isinstance(rows, list)
    assert len(rows) == 3
    assert set(rows[0].keys()) >= {"id", "name"}


@pytest.mark.asyncio
async def test_readjson_bigdata(schema_definition, metrics):
    comp = ReadJSON(
        name="ReadJSON_BigData",
        description="Read NDJSON with Dask",
        comp_type="read_json",
        filepath=VALID_NDJSON,
        schema=schema_definition
    )
    comp.strategy = BigDataExecutionStrategy()

    ddf = await comp.execute(payload=None, metrics=metrics)
    assert isinstance(ddf, dd.DataFrame)
    df = ddf.compute().sort_values("id")
    assert len(df) == 3
    assert {"Alice", "Bob", "Charlie"}.issubset(set(df["name"]))


@pytest.mark.asyncio
async def test_write_json_row(tmp_path: Path, schema_definition, metrics):
    out_fp = tmp_path / "single.json"

    comp = WriteJSON(
        name="WriteJSON_Row",
        description="Write single row",
        comp_type="write_json",
        filepath=out_fp,
        schema=schema_definition
    )
    comp.strategy = RowExecutionStrategy()

    row = {"id": 1, "name": "Zoe"}
    result = await comp.execute(payload=row, metrics=metrics)

    assert result[0] == row

    assert out_fp.exists()
    content = json.loads(out_fp.read_text(encoding="utf-8"))
    assert row in content

@pytest.mark.asyncio
async def test_writejson_bulk(tmp_path: Path, schema_definition, metrics):
    out_fp = tmp_path / "bulk.json"

    comp = WriteJSON(
        name="WriteJSON_Bulk",
        description="Write list of records",
        comp_type="write_json",
        filepath=out_fp,
        schema=schema_definition
    )
    comp.strategy = BulkExecutionStrategy()

    data = [
        {"id": 1, "name": "A"},
        {"id": 2, "name": "B"},
        {"id": 3, "name": "C"},
    ]
    res = await comp.execute(payload=data, metrics=metrics)
    assert isinstance(res, list) and len(res) == 3
    assert out_fp.exists()

    reader = ReadJSON(
        name="ReadBack_Bulk_JSON",
        description="Read back bulk JSON",
        comp_type="read_json",
        filepath=out_fp,
        schema=schema_definition
    )
    reader.strategy = BulkExecutionStrategy()
    df = await reader.execute(payload=None, metrics=metrics)
    assert list(df.sort_values("id")["name"]) == ["A", "B", "C"]


@pytest.mark.asyncio
async def test_writejson_bigdata(tmp_path: Path, schema_definition, metrics):
    out_dir = tmp_path / "big_out"
    out_dir.mkdir(parents=True, exist_ok=True)

    comp = WriteJSON(
        name="WriteJSON_BigData",
        description="Write Dask DataFrame as partitioned NDJSON",
        comp_type="write_json",
        filepath=out_dir,
        schema=schema_definition
    )
    comp.strategy = BigDataExecutionStrategy()

    ddf_in = dd.from_pandas(
        pd.DataFrame(
            [
                {"id": 10, "name": "Nina"},
                {"id": 11, "name": "Omar"},
            ]
        ),
        npartitions=2,
    )

    result = await comp.execute(payload=ddf_in, metrics=metrics)
    assert isinstance(result, dd.DataFrame)

    parts = sorted(out_dir.glob("part-*.json"))
    assert parts, "No partition files written."

    ddf_out = dd.read_json([str(p) for p in parts], lines=True, blocksize="64MB")
    df_out = ddf_out.compute().sort_values("id")
    assert list(df_out["name"]) == ["Nina", "Omar"]
