from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import dask.dataframe as dd
import pandas as pd
import pytest

from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.strategies.bigdata_strategy import BigDataExecutionStrategy
from etl_core.strategies.bulk_strategy import BulkExecutionStrategy
from etl_core.strategies.row_strategy import RowExecutionStrategy

from etl_core.components.file_components.json.read_json_component import ReadJSON
from etl_core.components.file_components.json.write_json_component import WriteJSON

DATA_DIR = Path(__file__).parent / "data" / "json"
VALID_JSON = DATA_DIR / "testdata.json"
NESTED_JSON = DATA_DIR / "testdata_nested.json"
BAD_JSON = DATA_DIR / "testdata_bad.json"
EXTRA_MISSING_JSON = DATA_DIR / "testdata_extra_missing.json"
MIXED_TYPES_JSON = DATA_DIR / "testdata_mixed_types.json"

VALID_NDJSON = DATA_DIR / "testdata.jsonl"
BAD_LINE_NDJSON = DATA_DIR / "testdata_bad_line.jsonl"
MIXED_SCHEMA_NDJSON = DATA_DIR / "testdata_mixed_schema.jsonl"


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
async def test_read_row_ndjson_happy_path(metrics: ComponentMetrics) -> None:
    comp = ReadJSON(
        name="r1",
        description="read ndjson",
        comp_type="read_json",
        filepath=VALID_NDJSON,
    )
    comp.strategy = RowExecutionStrategy()

    collected: List[Dict[str, Any]] = []
    async for rec in comp.execute(payload=None, metrics=metrics):
        collected.append(rec)

    assert collected == [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ]
    assert metrics.lines_received == 3
    assert metrics.error_count == 0


@pytest.mark.asyncio
async def test_read_row_ndjson_with_bad_line_skips_and_counts_error(
    metrics: ComponentMetrics,
) -> None:
    comp = ReadJSON(
        name="r2",
        description="read ndjson with a bad line",
        comp_type="read_json",
        filepath=BAD_LINE_NDJSON,
    )
    comp.strategy = RowExecutionStrategy()

    collected: List[Dict[str, Any]] = []
    async for rec in comp.execute(payload=None, metrics=metrics):
        collected.append(rec)

    assert collected == [
        {"id": 1, "name": "Alice"},
        {"id": 3, "name": "Charlie"},
    ]
    assert metrics.lines_received == 2
    assert metrics.error_count == 1


@pytest.mark.asyncio
async def test_read_row_ndjson_mixed_schema(metrics: ComponentMetrics) -> None:
    comp = ReadJSON(
        name="r3",
        description="read ndjson mixed schema",
        comp_type="read_json",
        filepath=MIXED_SCHEMA_NDJSON,
    )
    comp.strategy = RowExecutionStrategy()

    out: List[Dict[str, Any]] = []
    async for rec in comp.execute(payload=None, metrics=metrics):
        out.append(rec)

    assert out == [
        {"id": 1, "name": "Alice"},
        {"id": 2, "nickname": "Bobby"},
        {"id": 3, "name": "Charlie", "active": True},
    ]
    assert metrics.lines_received == 3
    assert metrics.error_count == 0


@pytest.mark.asyncio
async def test_read_bulk_json_array(metrics: ComponentMetrics) -> None:
    comp = ReadJSON(
        name="rb1",
        description="read json array",
        comp_type="read_json",
        filepath=VALID_JSON,
    )
    comp.strategy = BulkExecutionStrategy()

    dfs: List[pd.DataFrame] = []
    async for df in comp.execute(payload=None, metrics=metrics):
        dfs.append(df)

    assert len(dfs) == 1
    df = dfs[0].sort_values("id").reset_index(drop=True)
    expected = (
        pd.DataFrame(
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
                {"id": 3, "name": "Charlie"},
            ]
        )
        .sort_values("id")
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(df, expected, check_dtype=False)
    assert metrics.lines_received == 3
    assert metrics.error_count == 0


@pytest.mark.asyncio
async def test_read_bulk_json_array_nested_and_nulls(metrics: ComponentMetrics) -> None:
    comp = ReadJSON(
        name="rb2",
        description="read nested json array",
        comp_type="read_json",
        filepath=NESTED_JSON,
    )
    comp.strategy = BulkExecutionStrategy()

    dfs: List[pd.DataFrame] = []
    async for df in comp.execute(payload=None, metrics=metrics):
        dfs.append(df)

    assert len(dfs) == 1
    df = dfs[0].sort_values("id").reset_index(drop=True)

    assert df.shape[0] == 3
    assert list(df["id"]) == [1, 2, 3]
    assert isinstance(df.loc[0, "addr"], dict)
    assert isinstance(df.loc[1, "addr"], dict)
    assert pd.isna(df.loc[2, "addr"]) or df.loc[2, "addr"] is None

    assert metrics.lines_received == 3
    assert metrics.error_count == 0


@pytest.mark.asyncio
async def test_read_bulk_broken_json_raises_and_counts_error(
    metrics: ComponentMetrics,
) -> None:
    comp = ReadJSON(
        name="rb3",
        description="broken json should error",
        comp_type="read_json",
        filepath=BAD_JSON,
    )
    comp.strategy = BulkExecutionStrategy()

    with pytest.raises(Exception):
        gen = comp.execute(payload=None, metrics=metrics)
        await anext(gen)

    assert metrics.lines_received == 0
    assert metrics.error_count >= 1


@pytest.mark.asyncio
async def test_read_bulk_extra_and_missing_columns(metrics: ComponentMetrics) -> None:
    comp = ReadJSON(
        name="rb4",
        description="extra / missing fields",
        comp_type="read_json",
        filepath=EXTRA_MISSING_JSON,
    )
    comp.strategy = BulkExecutionStrategy()

    dfs: List[pd.DataFrame] = []
    async for df in comp.execute(payload=None, metrics=metrics):
        dfs.append(df)

    assert len(dfs) == 1
    df = dfs[0].sort_values("id").reset_index(drop=True)

    assert set(df.columns) == {"id", "name", "age", "city", "nickname"}
    row1 = df[df["id"] == 1].iloc[0].to_dict()
    assert row1["name"] == "Alice" and row1["age"] == 30 and row1["city"] == "Berlin"
    row2 = df[df["id"] == 2].iloc[0].to_dict()
    assert row2["name"] == "Bob" and pd.isna(row2["age"]) and pd.isna(row2["nickname"])
    row3 = df[df["id"] == 3].iloc[0].to_dict()
    assert (
        row3["nickname"] == "Charlie" and pd.isna(row3["name"]) and pd.isna(row3["age"])
    )

    assert metrics.lines_received == 3
    assert metrics.error_count == 0


@pytest.mark.asyncio
async def test_read_bulk_mixed_types(metrics: ComponentMetrics) -> None:
    comp = ReadJSON(
        name="rb5",
        description="mixed numeric/string/null",
        comp_type="read_json",
        filepath=MIXED_TYPES_JSON,
    )
    comp.strategy = BulkExecutionStrategy()

    dfs: List[pd.DataFrame] = []
    async for df in comp.execute(payload=None, metrics=metrics):
        dfs.append(df)

    assert len(dfs) == 1
    df = dfs[0].sort_values("id").reset_index(drop=True)
    scores = list(df["score"])
    assert scores[0] == 95
    assert scores[1] == 88
    assert pd.isna(scores[2])
    assert metrics.lines_received == 3
    assert metrics.error_count == 0


@pytest.mark.asyncio
async def test_read_bigdata_from_ndjson(metrics: ComponentMetrics) -> None:
    comp = ReadJSON(
        name="bg1",
        description="bigdata ndjson",
        comp_type="read_json",
        filepath=VALID_NDJSON,
    )
    comp.strategy = BigDataExecutionStrategy()

    ddfs: List[dd.DataFrame] = []
    async for ddf in comp.execute(payload=None, metrics=metrics):
        ddfs.append(ddf)

    assert len(ddfs) == 1
    df = ddfs[0].compute().sort_values("id").reset_index(drop=True)
    expected = (
        pd.DataFrame(
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
                {"id": 3, "name": "Charlie"},
            ]
        )
        .sort_values("id")
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(df, expected, check_dtype=False)
    assert metrics.lines_received == 3
    assert metrics.error_count == 0


@pytest.mark.asyncio
async def test_write_row_to_ndjson_and_read_back(
    metrics: ComponentMetrics, tmp_path: Path
) -> None:
    out = tmp_path / "out.jsonl"
    writer = WriteJSON(
        name="w1",
        description="write ndjson rows",
        comp_type="write_json",
        filepath=out,
    )
    writer.strategy = RowExecutionStrategy()

    rows = [
        {"id": 1, "name": "Alice"},
        {"id": 3, "name": "Charlie"},
    ]
    for r in rows:
        gen = writer.execute(payload=r, metrics=metrics)
        yielded = await anext(gen)
        assert yielded == r

    read_back: List[Dict[str, Any]] = []
    for line in out.read_text(encoding="utf-8").splitlines():
        if line.strip():
            read_back.append(json.loads(line))

    assert read_back == rows
    assert metrics.lines_received == 2
    assert metrics.error_count == 0

    reader = ReadJSON(
        name="rback",
        description="read back ndjson",
        comp_type="read_json",
        filepath=out,
    )
    reader.strategy = RowExecutionStrategy()

    collected: List[Dict[str, Any]] = []
    async for rec in reader.execute(
        payload=None,
        metrics=ComponentMetrics(
            started_at=datetime.now(),
            processing_time=timedelta(0),
            error_count=0,
            lines_received=0,
            lines_forwarded=0,
        ),
    ):
        collected.append(rec)
    assert collected == rows


@pytest.mark.asyncio
async def test_write_bulk_to_json_array_and_read_back(
    tmp_path: Path, metrics: ComponentMetrics
) -> None:
    out = tmp_path / "out.json"
    writer = WriteJSON(
        name="w2",
        description="bulk to array",
        comp_type="write_json",
        filepath=out,
    )
    writer.strategy = BulkExecutionStrategy()

    df = pd.DataFrame(
        [
            {"id": 10, "name": "Zoe"},
            {"id": 11, "name": "Liam"},
            {"id": 9, "name": "Ada"},
        ]
    )

    gen = writer.execute(payload=df, metrics=metrics)
    got = await anext(gen)
    pd.testing.assert_frame_equal(
        got.sort_values("id"), df.sort_values("id"), check_dtype=False
    )

    on_disk = json.loads(out.read_text(encoding="utf-8"))
    assert isinstance(on_disk, list)
    assert sorted(on_disk, key=lambda x: x["id"]) == sorted(
        df.to_dict(orient="records"), key=lambda x: x["id"]
    )
    assert metrics.lines_received == len(df)
    assert metrics.error_count == 0


@pytest.mark.asyncio
async def test_write_bulk_to_ndjson_lines_and_roundtrip(
    tmp_path: Path, metrics: ComponentMetrics
) -> None:
    out = tmp_path / "out.jsonl"
    writer = WriteJSON(
        name="w3",
        description="bulk -> ndjson",
        comp_type="write_json",
        filepath=out,
    )
    writer.strategy = BulkExecutionStrategy()

    df = pd.DataFrame([{"id": 2, "name": "b"}, {"id": 1, "name": "a"}])
    gen = writer.execute(payload=df, metrics=metrics)
    _ = await anext(gen)

    raw = [
        json.loads(s) for s in out.read_text(encoding="utf-8").splitlines() if s.strip()
    ]
    assert sorted(raw, key=lambda x: x["id"]) == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
    ]

    back = pd.read_json(out, lines=True)
    pd.testing.assert_frame_equal(
        back.sort_values("id").reset_index(drop=True),
        df.sort_values("id").reset_index(drop=True),
        check_dtype=False,
    )
    assert metrics.lines_received == len(df)
    assert metrics.error_count == 0
