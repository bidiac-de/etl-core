from __future__ import annotations

import json
import asyncio
import inspect
from datetime import datetime, timedelta
from pathlib import Path
from typing import AsyncGenerator, Any, Dict, List

import dask.dataframe as dd
import pandas as pd
import pytest

from etl_core.components.envelopes import Out
from etl_core.components.file_components.json.read_json import ReadJSON
from etl_core.components.file_components.json.write_json import WriteJSON
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.strategies.bigdata_strategy import BigDataExecutionStrategy
from etl_core.strategies.bulk_strategy import BulkExecutionStrategy
from etl_core.strategies.row_strategy import RowExecutionStrategy

DATA_DIR = Path(__file__).parent.parent / "data" / "json"
VALID_JSON = DATA_DIR / "testdata.json"
NESTED_JSON = DATA_DIR / "testdata_nested.json"
BAD_JSON = DATA_DIR / "testdata_bad.json"
EXTRA_MISSING_JSON = DATA_DIR / "testdata_extra_missing.json"
MIXED_TYPES_JSON = DATA_DIR / "testdata_mixed_types.json"

VALID_NDJSON = DATA_DIR / "testdata.jsonl"
BAD_LINE_NDJSON = DATA_DIR / "testdata_bad_line.jsonl"
MIXED_SCHEMA_NDJSON = DATA_DIR / "testdata_mixed_schema.jsonl"


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
    async for item in comp.execute(payload=None, metrics=metrics):
        assert isinstance(item, Out) and item.port == "out"
        collected.append(item.payload)

    assert collected == [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ]
    assert metrics.lines_forwarded == 3
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
    async for item in comp.execute(payload=None, metrics=metrics):
        assert isinstance(item, Out) and item.port == "out"
        collected.append(item.payload)

    assert collected == [
        {"id": 1, "name": "Alice"},
        {"id": 3, "name": "Charlie"},
    ]
    assert metrics.lines_forwarded == 2
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
    async for item in comp.execute(payload=None, metrics=metrics):
        assert isinstance(item, Out) and item.port == "out"
        out.append(item.payload)

    assert out == [
        {"id": 1, "name": "Alice"},
        {"id": 2, "nickname": "Bobby"},
        {"id": 3, "name": "Charlie", "active": True},
    ]
    assert metrics.lines_forwarded == 3
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

    frames: List[pd.DataFrame] = []
    async for item in comp.execute(payload=None, metrics=metrics):
        assert isinstance(item, Out) and item.port == "out"
        frames.append(item.payload)

    assert len(frames) == 1
    df = frames[0].sort_values("id").reset_index(drop=True)
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
    assert metrics.lines_forwarded == 3
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

    frames: List[pd.DataFrame] = []
    async for item in comp.execute(payload=None, metrics=metrics):
        assert isinstance(item, Out) and item.port == "out"
        frames.append(item.payload)

    assert len(frames) == 1
    df = frames[0].sort_values("id").reset_index(drop=True)

    assert df.shape[0] == 3
    assert list(df["id"]) == [1, 2, 3]
    # <- city/zip statt street
    assert {"addr.city", "addr.zip"} <= set(df.columns)

    assert df.loc[0, "addr.city"] == "Berlin"
    assert df.loc[0, "addr.zip"] == "10115"
    assert df.loc[1, "addr.city"] == "Hamburg"
    assert df.loc[1, "addr.zip"] == "20095"
    assert pd.isna(df.loc[2, "addr.city"])
    assert pd.isna(df.loc[2, "addr.zip"])

    assert not any(isinstance(v, dict) for v in df.to_numpy().ravel())

    assert metrics.lines_forwarded == 3
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

    frames: List[pd.DataFrame] = []
    async for item in comp.execute(payload=None, metrics=metrics):
        assert isinstance(item, Out) and item.port == "out"
        frames.append(item.payload)

    assert len(frames) == 1
    df = frames[0].sort_values("id").reset_index(drop=True)

    assert set(df.columns) == {"id", "name", "age", "city", "nickname"}
    row1 = df[df["id"] == 1].iloc[0].to_dict()
    assert row1["name"] == "Alice" and row1["age"] == 30 and row1["city"] == "Berlin"
    row2 = df[df["id"] == 2].iloc[0].to_dict()
    assert row2["name"] == "Bob" and pd.isna(row2["age"]) and pd.isna(row2["nickname"])
    row3 = df[df["id"] == 3].iloc[0].to_dict()
    assert (
        row3["nickname"] == "Charlie" and pd.isna(row3["name"]) and pd.isna(row3["age"])
    )

    assert metrics.lines_forwarded == 3
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

    frames: List[pd.DataFrame] = []
    async for item in comp.execute(payload=None, metrics=metrics):
        assert isinstance(item, Out) and item.port == "out"
        frames.append(item.payload)

    assert len(frames) == 1
    df = frames[0].sort_values("id").reset_index(drop=True)
    scores = list(df["score"])
    assert scores[0] == 95
    assert scores[1] == "88"
    assert pd.isna(scores[2])
    assert metrics.lines_forwarded == 3
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
    async for item in comp.execute(payload=None, metrics=metrics):
        assert isinstance(item, Out) and item.port == "out"
        ddfs.append(item.payload)

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
    assert metrics.lines_forwarded == 3
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
        assert isinstance(yielded, Out) and yielded.port == "out"
        assert yielded.payload == r

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
    async for item in reader.execute(
        payload=None,
        metrics=ComponentMetrics(
            started_at=datetime.now(),
            processing_time=timedelta(0),
            error_count=0,
            lines_received=0,
            lines_forwarded=0,
        ),
    ):
        assert isinstance(item, Out) and item.port == "out"
        collected.append(item.payload)
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
    assert isinstance(got, Out) and got.port == "out"
    pd.testing.assert_frame_equal(
        got.payload.sort_values("id"),
        df.sort_values("id"),
        check_dtype=False,
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
    item = await anext(gen)
    assert isinstance(item, Out) and item.port == "out"

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


@pytest.mark.asyncio
async def test_read_row_ndjson_missing_file_raises(
    metrics: ComponentMetrics, tmp_path: Path
) -> None:
    comp = ReadJSON(
        name="r_missing",
        description="missing file should error",
        comp_type="read_json",
        filepath=tmp_path / "does_not_exist.jsonl",
    )
    comp.strategy = RowExecutionStrategy()

    with pytest.raises(FileNotFoundError):
        gen = comp.execute(payload=None, metrics=metrics)
        await anext(gen)


@pytest.mark.asyncio
async def test_read_json_row_streaming_nested_returns_nested(metrics) -> None:
    comp = ReadJSON(
        name="read_row_nested",
        description="row nested",
        comp_type="read_json",
        filepath=NESTED_JSON,
    )
    comp.strategy = RowExecutionStrategy()

    rows = comp.execute(payload=None, metrics=metrics)
    assert inspect.isasyncgen(rows) or isinstance(rows, AsyncGenerator)

    first: Out = await asyncio.wait_for(anext(rows), timeout=0.5)
    assert isinstance(first, Out) and first.port == "out"
    payload = first.payload
    assert isinstance(payload, dict)
    assert "addr" in payload and isinstance(payload["addr"], dict)
    assert set(payload.keys()) >= {"id", "addr"}

    async for _ in rows:
        pass

    await rows.aclose()


@pytest.mark.asyncio
async def test_read_json_bulk_flattens(metrics) -> None:
    comp = ReadJSON(
        name="read_bulk_flattens",
        description="bulk flatten",
        comp_type="read_json",
        filepath=NESTED_JSON,
    )
    comp.strategy = BulkExecutionStrategy()

    chunks: List[pd.DataFrame] = []
    async for item in comp.execute(payload=None, metrics=metrics):
        assert isinstance(item, Out) and isinstance(item.payload, pd.DataFrame)
        chunks.append(item.payload)

    assert chunks
    df = pd.concat(chunks, ignore_index=True)

    assert {"id", "addr.city", "addr.zip"} <= set(df.columns)
    assert not any(isinstance(v, dict) for v in df.to_numpy().ravel())


@pytest.mark.asyncio
async def test_read_json_bigdata_flattens_partitions(metrics, tmp_path: Path) -> None:
    ndjson = tmp_path / "nested.jsonl"
    lines = [
        {"id": 1, "addr": {"street": "Main", "city": "Town"}},
        {"id": 2, "addr": {"street": "Second", "city": "Ville"}},
    ]
    with ndjson.open("w", encoding="utf-8") as f:
        for r in lines:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    comp = ReadJSON(
        name="read_big_nested",
        description="bigdata nested",
        comp_type="read_json",
        filepath=ndjson,
    )
    comp.strategy = BigDataExecutionStrategy()

    outs: List[dd.DataFrame] = []
    async for item in comp.execute(payload=None, metrics=metrics):
        outs.append(item.payload)

    assert len(outs) == 1
    ddf = outs[0]
    df = ddf.compute().sort_values("id").reset_index(drop=True)
    assert {"id", "addr.street", "addr.city"} <= set(df.columns)
    assert list(df["addr.street"]) == ["Main", "Second"]


@pytest.mark.asyncio
async def test_write_json_row_accepts_nested_and_appends(
    metrics, tmp_path: Path
) -> None:
    out = tmp_path / "rows.json"
    comp = WriteJSON(
        name="write_row_nested_ok",
        description="row nested ok",
        comp_type="write_json",
        filepath=out,
    )
    comp.strategy = RowExecutionStrategy()

    r1 = {"id": 1, "addr": {"street": "Main", "city": "Town"}}
    r2 = {"id": 2, "addr": {"street": "Second", "city": "Ville"}}

    got1 = await anext(comp.execute(payload=r1, metrics=metrics))
    got2 = await anext(comp.execute(payload=r2, metrics=metrics))
    assert isinstance(got1, Out) and got1.payload == r1
    assert isinstance(got2, Out) and got2.payload == r2

    on_disk = json.loads(out.read_text(encoding="utf-8"))
    assert on_disk == [r1, r2]


@pytest.mark.asyncio
async def test_write_json_bulk_unflattens_from_flat_df(metrics, tmp_path: Path) -> None:
    out = tmp_path / "bulk.json"
    comp = WriteJSON(
        name="write_bulk_unflatten",
        description="bulk unflatten",
        comp_type="write_json",
        filepath=out,
    )
    comp.strategy = BulkExecutionStrategy()

    df = pd.DataFrame(
        [
            {
                "id": 1,
                "addr.street": "Main",
                "addr.city": "Town",
                "tags[0]": "t1",
                "tags[1]": "t2",
            },
            {"id": 2, "addr.street": "Second", "addr.city": "Ville", "tags[0]": "u1"},
        ]
    )

    yielded = await anext(comp.execute(payload=df, metrics=metrics))
    assert isinstance(yielded, Out) and yielded.port == "out"

    data = json.loads(out.read_text(encoding="utf-8"))
    assert data[0]["addr"] == {"street": "Main", "city": "Town"}
    assert data[0]["tags"] == ["t1", "t2"]
    assert data[1]["addr"] == {"street": "Second", "city": "Ville"}
    assert data[1]["tags"] == ["u1"]


@pytest.mark.asyncio
async def test_write_json_bigdata_partitioned_ndjson(metrics, tmp_path: Path) -> None:
    out_dir = tmp_path / "bigdata_out"
    comp = WriteJSON(
        name="write_bigdata_partitioned",
        description="bigdata partitioned ndjson",
        comp_type="write_json",
        filepath=out_dir,
    )
    comp.strategy = BigDataExecutionStrategy()

    pdf = pd.DataFrame(
        [
            {"id": 10, "addr.street": "Alpha", "addr.city": "A-Town"},
            {"id": 11, "addr.street": "Beta", "addr.city": "B-City"},
        ]
    )
    ddf = dd.from_pandas(pdf, npartitions=2)

    yielded = await anext(comp.execute(payload=ddf, metrics=metrics))
    assert isinstance(yielded, Out) and yielded.port == "out"

    parts = sorted(out_dir.glob("part-*.jsonl*"))
    assert parts, "no NDJSON part files written"

    rows = []
    for p in parts:
        for line in p.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if s:
                rows.append(json.loads(s))

    assert rows == [
        {"id": 10, "addr": {"street": "Alpha", "city": "A-Town"}},
        {"id": 11, "addr": {"street": "Beta", "city": "B-City"}},
    ]

    assert metrics.error_count == 0
    assert metrics.lines_received == 2
    assert metrics.lines_forwarded == 2
