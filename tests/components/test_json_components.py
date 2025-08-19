import json
import pytest
import pandas as pd
import dask.dataframe as dd
from datetime import datetime, timedelta
from pathlib import Path
from typing import AsyncGenerator

from etl_core.strategies.row_strategy import RowExecutionStrategy
from etl_core.strategies.bulk_strategy import BulkExecutionStrategy
from etl_core.strategies.bigdata_strategy import BigDataExecutionStrategy

from etl_core.components.file_components.json.read_json_component import ReadJSON
from etl_core.components.file_components.json.write_json_component import WriteJSON

from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics


DATA_DIR = Path(__file__).parent / "data" / "json"
VALID_JSON = DATA_DIR / "testdata.json"
VALID_NDJSON = DATA_DIR / "testdata.jsonl"
EXTRA_MISSING_JSON = DATA_DIR / "testdata_extra_missing.json"
MIXED_TYPES_JSON = DATA_DIR / "testdata_mixed_types.json"
NESTED_JSON = DATA_DIR / "testdata_nested.json"
INVALID_JSON_FILE = DATA_DIR / "testdata_bad.json"
BAD_LINE_JSONL = DATA_DIR / "testdata_bad_line.jsonl"
MIXED_SCHEMA_JSONL = DATA_DIR / "testdata_mixed_schema.jsonl"


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
async def test_read_json_valid_bulk(metrics):
    comp = ReadJSON(
        name="ReadJSON_Bulk_Valid",
        description="Valid JSON array-of-records",
        comp_type="read_json",
        filepath=VALID_JSON,
    )
    comp.strategy = BulkExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)
    assert isinstance(res, AsyncGenerator)

    async for df in res:
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert {"id", "name"}.issubset(df.columns)


@pytest.mark.asyncio
async def test_read_json_ndjson_bulk(metrics):
    comp = ReadJSON(
        name="ReadJSON_Bulk_NDJSON",
        description="Valid NDJSON",
        comp_type="read_json",
        filepath=VALID_NDJSON,
    )
    comp.strategy = BulkExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)
    async for df in res:
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert {"id", "name"}.issubset(df.columns)


@pytest.mark.asyncio
async def test_read_json_invalid_json_content_raises(metrics):
    comp = ReadJSON(
        name="ReadJSON_Invalid_Content",
        description="Malformed JSON",
        comp_type="read_json",
        filepath=INVALID_JSON_FILE,
    )
    comp.strategy = BulkExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)
    with pytest.raises(Exception):
        await anext(res)


@pytest.mark.asyncio
async def test_read_json_bulk_extra_missing(metrics):
    comp = ReadJSON(
        name="ReadJSON_ExtraMissing",
        description="Extra + missing fields",
        comp_type="read_json",
        filepath=EXTRA_MISSING_JSON,
    )
    comp.strategy = BulkExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)
    async for df in res:
        assert isinstance(df, pd.DataFrame)
        assert {"id", "name"}.issubset(df.columns)
        assert {"age", "city", "nickname"}.issubset(df.columns)


@pytest.mark.asyncio
async def test_read_json_bulk_mixed_types(metrics):
    comp = ReadJSON(
        name="ReadJSON_MixedTypes",
        description="Mixed numeric/string/null",
        comp_type="read_json",
        filepath=MIXED_TYPES_JSON,
    )
    comp.strategy = BulkExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)
    async for df in res:
        assert "score" in df.columns
        pd.to_numeric(df["score"], errors="coerce")


@pytest.mark.asyncio
async def test_read_json_bulk_nested(metrics):
    comp = ReadJSON(
        name="ReadJSON_Nested",
        description="Keep nested dicts",
        comp_type="read_json",
        filepath=NESTED_JSON,
    )
    comp.strategy = BulkExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)
    async for df in res:
        assert "addr" in df.columns
        assert isinstance(df.iloc[0]["addr"], dict) or df.iloc[0]["addr"] is None


@pytest.mark.asyncio
async def test_read_json_ndjson_bad_line_raises(metrics):
    comp = ReadJSON(
        name="ReadJSON_NDJSON_BadLine",
        description="Bad line in NDJSON",
        comp_type="read_json",
        filepath=BAD_LINE_JSONL,
    )
    comp.strategy = BulkExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)
    with pytest.raises(Exception):
        await anext(res)


@pytest.mark.asyncio
async def test_read_json_ndjson_mixed_schema(metrics):
    comp = ReadJSON(
        name="ReadJSON_NDJSON_MixedSchema",
        description="Varying schema in NDJSON",
        comp_type="read_json",
        filepath=MIXED_SCHEMA_JSONL,
    )
    comp.strategy = BulkExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)
    async for df in res:
        assert {"id", "name", "nickname", "active"}.issubset(df.columns)


@pytest.mark.asyncio
async def test_read_json_row_streaming(metrics):
    comp = ReadJSON(
        name="ReadJSON_Row_Stream",
        description="Row streaming over JSON array",
        comp_type="read_json",
        filepath=VALID_JSON,
    )
    comp.strategy = RowExecutionStrategy()

    rows = comp.execute(payload=None, metrics=metrics)
    assert isinstance(rows, AsyncGenerator)

    first = await anext(rows)
    assert {"id", "name"}.issubset(first.keys())

    second = await anext(rows)
    assert {"id", "name"}.issubset(second.keys())

    await rows.aclose()


@pytest.mark.asyncio
async def test_read_json_bigdata(metrics):
    comp = ReadJSON(
        name="ReadJSON_BigData",
        description="Read NDJSON with Dask",
        comp_type="read_json",
        filepath=VALID_NDJSON,
    )
    comp.strategy = BigDataExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)
    async for ddf in res:
        assert isinstance(ddf, dd.DataFrame)
        df = ddf.compute().sort_values("id")
        assert len(df) == 3
        assert {"Alice", "Bob", "Charlie"}.issubset(set(df["name"]))


@pytest.mark.asyncio
async def test_write_json_row(tmp_path: Path, metrics):
    out_fp = tmp_path / "single.json"

    comp = WriteJSON(
        name="WriteJSON_Row",
        description="Write single row",
        comp_type="write_json",
        filepath=out_fp,
    )
    comp.strategy = RowExecutionStrategy()

    row = {"id": 1, "name": "Zoe"}
    res = comp.execute(payload=row, metrics=metrics)

    yielded = await anext(res)
    assert yielded == row

    assert out_fp.exists()
    content = json.loads(out_fp.read_text(encoding="utf-8"))
    assert row in content


@pytest.mark.asyncio
async def test_write_json_bulk(tmp_path: Path, metrics):
    out_fp = tmp_path / "bulk.json"

    comp = WriteJSON(
        name="WriteJSON_Bulk",
        description="Write list of records",
        comp_type="write_json",
        filepath=out_fp,
    )
    comp.strategy = BulkExecutionStrategy()

    data = pd.DataFrame(
        [
            {"id": 1, "name": "A"},
            {"id": 2, "name": "B"},
            {"id": 3, "name": "C"},
        ]
    )
    gen = comp.execute(payload=data, metrics=metrics)
    await anext(gen, None)

    assert out_fp.exists()
    reader = ReadJSON(
        name="ReadBack_Bulk_JSON",
        description="Read back bulk JSON",
        comp_type="read_json",
        filepath=out_fp,
    )
    reader.strategy = BulkExecutionStrategy()

    res = reader.execute(payload=None, metrics=metrics)
    async for df in res:
        assert list(df.sort_values("id")["name"]) == ["A", "B", "C"]


@pytest.mark.asyncio
async def test_write_json_bigdata(tmp_path: Path, metrics):
    out_dir = tmp_path / "big_out"
    out_dir.mkdir(parents=True, exist_ok=True)

    comp = WriteJSON(
        name="WriteJSON_BigData",
        description="Write Dask DataFrame as partitioned NDJSON",
        comp_type="write_json",
        filepath=out_dir,
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

    gen = comp.execute(payload=ddf_in, metrics=metrics)
    await anext(gen, None)

    parts = sorted(out_dir.glob("part-*.jsonl"))
    assert parts, "No partition files written."

    ddf_out = dd.read_json([str(p) for p in parts], lines=True, blocksize="64MB")
    df_out = ddf_out.compute().sort_values("id")
    assert list(df_out["name"]) == ["Nina", "Omar"]
