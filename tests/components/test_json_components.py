import json
import pytest
import pandas as pd
import dask.dataframe as dd
from datetime import datetime, timedelta
from pathlib import Path
from typing import AsyncGenerator, List, Dict, Any

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
async def test_component_read_json_row_exact(metrics: ComponentMetrics):
    comp = ReadJSON(
        name="ReadJSON_Row_Valid",
        description="Row exact read",
        comp_type="read_json",
        filepath=VALID_JSON,
    )
    comp.strategy = RowExecutionStrategy()

    gen = comp.execute(payload=None, metrics=metrics)
    assert isinstance(gen, AsyncGenerator)

    collected: List[Dict[str, Any]] = []
    async for rec in gen:
        collected.append(rec)

    expected = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ]
    assert collected == expected
    assert metrics.lines_received == len(expected)


@pytest.mark.asyncio
async def test_component_read_json_bulk_exact(metrics: ComponentMetrics):
    comp = ReadJSON(
        name="ReadJSON_Bulk_Valid",
        description="Bulk exact read",
        comp_type="read_json",
        filepath=VALID_JSON,
    )
    comp.strategy = BulkExecutionStrategy()

    gen = comp.execute(payload=None, metrics=metrics)
    dfs = []
    async for df in gen:
        dfs.append(df)
    assert len(dfs) == 1
    df = dfs[0].sort_values("id").reset_index(drop=True)
    expected_df = pd.DataFrame(
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]
    )
    pd.testing.assert_frame_equal(df, expected_df)
    assert metrics.lines_received == 3


@pytest.mark.asyncio
async def test_component_read_json_bigdata_exact(metrics: ComponentMetrics):
    comp = ReadJSON(
        name="ReadJSON_BigData_Valid",
        description="Bigdata NDJSON read",
        comp_type="read_json",
        filepath=VALID_NDJSON,
    )
    comp.strategy = BigDataExecutionStrategy()

    gen = comp.execute(payload=None, metrics=metrics)
    ddfs = []
    async for ddf in gen:
        ddfs.append(ddf)
    assert len(ddfs) == 1
    df = ddfs[0].compute().sort_values("id").reset_index(drop=True)
    expected_df = pd.DataFrame(
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]
    )
    pd.testing.assert_frame_equal(df, expected_df, check_dtype=False)
    assert metrics.lines_received == 3
    assert pd.api.types.is_integer_dtype(df["id"])
    assert df["name"].astype(str).map(type).eq(str).all()


@pytest.mark.asyncio
async def test_component_read_json_invalid_content_raises(metrics: ComponentMetrics):
    comp = ReadJSON(
        name="ReadJSON_Invalid",
        description="Malformed JSON",
        comp_type="read_json",
        filepath=INVALID_JSON_FILE,
    )
    comp.strategy = BulkExecutionStrategy()

    gen = comp.execute(payload=None, metrics=metrics)
    with pytest.raises(Exception):
        await anext(gen)


@pytest.mark.asyncio
async def test_component_read_json_ndjson_bad_line_raises(metrics: ComponentMetrics):
    comp = ReadJSON(
        name="ReadJSON_NDJSON_BadLine",
        description="NDJSON bad line",
        comp_type="read_json",
        filepath=BAD_LINE_JSONL,
    )
    comp.strategy = BulkExecutionStrategy()

    gen = comp.execute(payload=None, metrics=metrics)
    with pytest.raises(Exception):
        await anext(gen)


@pytest.mark.asyncio
async def test_component_read_json_bulk_extra_missing(metrics: ComponentMetrics):
    comp = ReadJSON(
        name="ReadJSON_ExtraMissing",
        description="Extra & missing fields",
        comp_type="read_json",
        filepath=EXTRA_MISSING_JSON,
    )
    comp.strategy = BulkExecutionStrategy()

    gen = comp.execute(payload=None, metrics=metrics)
    async for df in gen:
        cols = set(df.columns)
        assert {"id", "name"}.issubset(cols)
        assert {"age", "city", "nickname"} <= cols


@pytest.mark.asyncio
async def test_component_read_json_bulk_mixed_types(metrics: ComponentMetrics):
    comp = ReadJSON(
        name="ReadJSON_MixedTypes",
        description="Mixed column types",
        comp_type="read_json",
        filepath=MIXED_TYPES_JSON,
    )
    comp.strategy = BulkExecutionStrategy()

    gen = comp.execute(payload=None, metrics=metrics)
    async for df in gen:
        assert "score" in df.columns
        pd.to_numeric(df["score"], errors="coerce")


@pytest.mark.asyncio
async def test_component_read_json_bulk_nested(metrics: ComponentMetrics):
    comp = ReadJSON(
        name="ReadJSON_Nested",
        description="Nested fields kept",
        comp_type="read_json",
        filepath=NESTED_JSON,
    )
    comp.strategy = BulkExecutionStrategy()

    gen = comp.execute(payload=None, metrics=metrics)
    async for df in gen:
        assert "addr" in df.columns
        assert isinstance(df.iloc[0]["addr"], dict) or df.iloc[0]["addr"] is None


@pytest.mark.asyncio
async def test_component_read_json_ndjson_mixed_schema(metrics: ComponentMetrics):
    comp = ReadJSON(
        name="ReadJSON_NDJSON_MixedSchema",
        description="NDJSON varying schema",
        comp_type="read_json",
        filepath=MIXED_SCHEMA_JSONL,
    )
    comp.strategy = BulkExecutionStrategy()

    gen = comp.execute(payload=None, metrics=metrics)
    async for df in gen:
        assert {"id", "name"}.issubset(df.columns)
        assert {"nickname", "active"} <= set(df.columns)


@pytest.mark.asyncio
async def test_component_write_json_row(tmp_path: Path, metrics: ComponentMetrics):
    out_fp = tmp_path / "row.json"
    comp = WriteJSON(
        name="WriteJSON_Row",
        description="Write rows (append into JSON array)",
        comp_type="write_json",
        filepath=out_fp,
    )
    comp.strategy = RowExecutionStrategy()

    rows = [
        {"id": 10, "name": "Zoe"},
        {"id": 11, "name": "Liam"},
    ]
    for r in rows:
        gen = comp.execute(payload=r, metrics=metrics)
        yielded = await anext(gen)
        assert yielded == r

    assert metrics.lines_received == len(rows)
    content = json.loads(out_fp.read_text(encoding="utf-8"))
    assert content == rows

    read_metrics = ComponentMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
    )
    reader = ReadJSON(
        name="ReadBack_Row",
        description="Read back rows",
        comp_type="read_json",
        filepath=out_fp,
    )
    reader.strategy = BulkExecutionStrategy()
    gen2 = reader.execute(payload=None, metrics=read_metrics)
    dfs = []
    async for df in gen2:
        dfs.append(df)
    assert len(dfs) == 1
    df = dfs[0].sort_values("id").reset_index(drop=True)
    expected_df = pd.DataFrame(rows)
    pd.testing.assert_frame_equal(df, expected_df)


@pytest.mark.asyncio
async def test_component_write_json_bulk(tmp_path: Path, metrics: ComponentMetrics):
    out_fp = tmp_path / "bulk.json"
    comp = WriteJSON(
        name="WriteJSON_Bulk",
        description="Write DataFrame",
        comp_type="write_json",
        filepath=out_fp,
    )
    comp.strategy = BulkExecutionStrategy()

    data = pd.DataFrame(
        [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}, {"id": 3, "name": "C"}]
    )
    gen = comp.execute(payload=data, metrics=metrics)
    await anext(gen, None)

    assert metrics.lines_received == 3
    direct = json.loads(out_fp.read_text(encoding="utf-8"))
    assert direct == data.to_dict(orient="records")

    read_metrics = ComponentMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
    )
    reader = ReadJSON(
        name="ReadBack_Bulk",
        description="Read back bulk",
        comp_type="read_json",
        filepath=out_fp,
    )
    reader.strategy = BulkExecutionStrategy()
    gen2 = reader.execute(payload=None, metrics=read_metrics)
    dfs = []
    async for df in gen2:
        dfs.append(df)
    df_back = dfs[0].sort_values("id").reset_index(drop=True)
    pd.testing.assert_frame_equal(
        df_back, data.sort_values("id").reset_index(drop=True)
    )


@pytest.mark.asyncio
async def test_component_write_json_bigdata(tmp_path: Path, metrics: ComponentMetrics):
    out_dir = tmp_path / "big_out"
    out_dir.mkdir(parents=True, exist_ok=True)

    comp = WriteJSON(
        name="WriteJSON_BigData",
        description="Write Dask DF to partitioned NDJSON",
        comp_type="write_json",
        filepath=out_dir,
    )
    comp.strategy = BigDataExecutionStrategy()

    pdf = pd.DataFrame([{"id": 50, "name": "X"}, {"id": 51, "name": "Y"}])
    ddf_in = dd.from_pandas(pdf, npartitions=1)

    gen = comp.execute(payload=ddf_in, metrics=metrics)
    await anext(gen, None)

    assert metrics.lines_received == 2
    parts = sorted(out_dir.glob("part-*.jsonl"))
    assert parts, "Partition files missing"

    parsed: List[Dict[str, Any]] = []
    for p in parts:
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    parsed.append(json.loads(line))
    parsed_sorted = sorted(parsed, key=lambda x: x["id"])
    assert parsed_sorted == pdf.sort_values("id").to_dict(orient="records")

    ddf_out = dd.read_json([str(p) for p in parts], lines=True, blocksize="64MB")
    df_out = ddf_out.compute().sort_values("id").reset_index(drop=True)
    pd.testing.assert_frame_equal(
        df_out, pdf.sort_values("id").reset_index(drop=True), check_dtype=False
    )
