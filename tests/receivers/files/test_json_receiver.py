import asyncio
import inspect
from datetime import datetime, timedelta
from pathlib import Path
from typing import AsyncGenerator, List, Dict
import json

import dask.dataframe as dd
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from etl_core.receivers.files.json.json_receiver import JSONReceiver
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
def sample_json_file() -> Path:
    return (
        Path(__file__).parent.parent.parent
        / "components"
        / "data"
        / "json"
        / "testdata.json"
    )


@pytest.fixture
def sample_ndjson_file() -> Path:
    return (
        Path(__file__).parent.parent.parent
        / "components"
        / "data"
        / "json"
        / "testdata.jsonl"
    )


@pytest.mark.asyncio
async def test_read_json_row_exact(sample_json_file: Path, metrics: ComponentMetrics):
    r = JSONReceiver()
    rows_iter = r.read_row(filepath=sample_json_file, metrics=metrics)
    assert inspect.isasyncgen(rows_iter) or isinstance(rows_iter, AsyncGenerator)

    collected: List[Dict] = []
    async for rec in rows_iter:
        collected.append(rec)

    expected = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ]
    assert collected == expected, "order or content does not match"
    assert metrics.lines_forwarded == len(expected)


@pytest.mark.asyncio
async def test_read_json_bulk_exact(sample_json_file: Path, metrics: ComponentMetrics):
    r = JSONReceiver()
    df = await r.read_bulk(filepath=sample_json_file, metrics=metrics)

    expected_df = pd.DataFrame(
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]
    )
    df_sorted = df.sort_values("id").reset_index(drop=True)
    assert_frame_equal(df_sorted, expected_df)
    assert metrics.lines_forwarded == 3


@pytest.mark.asyncio
async def test_read_json_bigdata_exact(
    sample_ndjson_file: Path, metrics: ComponentMetrics
):
    r = JSONReceiver()
    ddf = await r.read_bigdata(filepath=sample_ndjson_file, metrics=metrics)
    assert isinstance(ddf, dd.DataFrame)

    df = ddf.compute().sort_values("id").reset_index(drop=True)
    expected_df = pd.DataFrame(
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]
    )
    assert_frame_equal(df, expected_df, check_dtype=False)
    assert metrics.lines_forwarded == 3
    assert pd.api.types.is_integer_dtype(df["id"])
    assert df["name"].astype(str).map(type).eq(str).all()


@pytest.mark.asyncio
async def test_write_json_row_and_re_read_single_row_mode(
    tmp_path: Path, metrics: ComponentMetrics
):
    file_path = tmp_path / "out_row.json"
    r = JSONReceiver()

    await r.write_row(
        filepath=file_path, metrics=metrics, row={"id": 10, "name": "Daisy"}
    )
    assert metrics.lines_received == 1

    assert file_path.exists(), "JSON output file was not created after first write"
    with file_path.open(encoding="utf-8") as f:
        after_first = json.load(f)
    assert after_first == [
        {"id": 10, "name": "Daisy"}
    ], "File should contain only Daisy after first write"

    await r.write_row(
        filepath=file_path, metrics=metrics, row={"id": 11, "name": "Eli"}
    )
    assert metrics.lines_forwarded == 2

    assert file_path.exists(), "JSON output file missing after second write"
    with file_path.open(encoding="utf-8") as f:
        after_second = json.load(f)
    expected_list = [
        {"id": 10, "name": "Daisy"},
        {"id": 11, "name": "Eli"},
    ]
    assert (
        after_second == expected_list
    ), "File should contain Daisy and Eli after second write"

    read_metrics = ComponentMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
    )
    df = await r.read_bulk(filepath=file_path, metrics=read_metrics)
    df_sorted = df.sort_values("id").reset_index(drop=True)
    expected_df = pd.DataFrame(expected_list)
    assert_frame_equal(df_sorted, expected_df)
    assert read_metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_write_json_bulk_exact(tmp_path: Path, metrics: ComponentMetrics):
    file_path = tmp_path / "out_bulk.json"
    r = JSONReceiver()

    data = pd.DataFrame(
        [
            {"id": 20, "name": "Finn"},
            {"id": 21, "name": "Gina"},
        ]
    )
    await r.write_bulk(filepath=file_path, metrics=metrics, data=data)
    assert metrics.lines_received == 2

    with file_path.open("r", encoding="utf-8") as f:
        direct_obj = json.load(f)
    assert isinstance(direct_obj, list)
    assert direct_obj == [
        {"id": 20, "name": "Finn"},
        {"id": 21, "name": "Gina"},
    ], "direct parsing (bulk array) does not match"

    read_metrics = ComponentMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
    )
    df = await r.read_bulk(filepath=file_path, metrics=read_metrics)
    df_sorted = df.sort_values("id").reset_index(drop=True)
    assert_frame_equal(df_sorted, data.sort_values("id").reset_index(drop=True))
    assert read_metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_write_json_bigdata_roundtrip(tmp_path: Path, metrics: ComponentMetrics):
    out_dir = tmp_path / "big_out"
    out_dir.mkdir(parents=True, exist_ok=True)
    r = JSONReceiver()

    pdf = pd.DataFrame(
        [
            {"id": 30, "name": "Hugo"},
            {"id": 31, "name": "Ivy"},
        ]
    )
    ddf_in = dd.from_pandas(pdf, npartitions=1)
    await r.write_bigdata(filepath=out_dir, metrics=metrics, data=ddf_in)

    assert metrics.lines_received == 2

    parts = sorted(out_dir.glob("part-*.jsonl"))
    assert parts, "No partitiones wrote"

    parsed_lines = []
    for p in parts:
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    parsed_lines.append(json.loads(line))
    parsed_lines_sorted = sorted(parsed_lines, key=lambda x: x["id"])
    assert parsed_lines_sorted == pdf.sort_values("id").to_dict(
        orient="records"
    ), "Direct parsing (bigdata) does not match"

    ddf_out = dd.read_json([str(p) for p in parts], lines=True, blocksize="64MB")
    df_out = ddf_out.compute().sort_values("id").reset_index(drop=True)
    assert_frame_equal(
        df_out, pdf.sort_values("id").reset_index(drop=True), check_dtype=False
    )


@pytest.mark.asyncio
async def test_read_json_row_gz(
    sample_json_file: Path, tmp_path: Path, metrics: ComponentMetrics
):
    """
    Test .gz read path (row base only), produces compromised file on-the-fly.
    """
    import gzip
    import json

    gz_path = tmp_path / "rows.json.gz"
    payload = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]

    def _write_gz():
        with gzip.open(gz_path, "wt", encoding="utf-8") as f:
            json.dump(payload, f)

    await asyncio.to_thread(_write_gz)

    r = JSONReceiver()
    rows_iter = r.read_row(filepath=gz_path, metrics=metrics)

    collected = []
    async for rec in rows_iter:
        collected.append(rec)

    assert collected == payload
    assert metrics.lines_forwarded == len(payload)


@pytest.mark.asyncio
async def test_read_bulk_missing_file_raises(metrics: ComponentMetrics, tmp_path: Path):
    r = JSONReceiver()
    missing = tmp_path / "missing.json"
    with pytest.raises(FileNotFoundError):
        await r.read_bulk(filepath=missing, metrics=metrics)


@pytest.mark.asyncio
async def test_read_row_nested_unflattens(tmp_path: Path, metrics: ComponentMetrics):
    path = tmp_path / "rows.jsonl"
    lines = [
        {"id": 1, "addr.street": "Main", "addr.city": "Town"},
        {"id": 2, "addr.street": "Second", "addr.city": "Ville"},
    ]
    with path.open("w", encoding="utf-8") as f:
        for r in lines:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    r = JSONReceiver()
    out: List[Dict] = []
    async for rec in r.read_row(filepath=path, metrics=metrics):
        out.append(rec)

    assert out[0]["addr"] == {"street": "Main", "city": "Town"}
    assert out[1]["addr"] == {"street": "Second", "city": "Ville"}


@pytest.mark.asyncio
async def test_read_bulk_flattens_nested(tmp_path: Path, metrics: ComponentMetrics):
    path = tmp_path / "bulk.json"
    payload = [
        {"id": 1, "addr": {"street": "Main", "city": "Town"}},
        {"id": 2, "addr": {"street": "Second", "city": "Ville"}},
    ]
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    r = JSONReceiver()
    df = await r.read_bulk(filepath=path, metrics=metrics)
    df = df.sort_values("id").reset_index(drop=True)

    expected = (
        pd.DataFrame(
            [
                {"id": 1, "addr.street": "Main", "addr.city": "Town"},
                {"id": 2, "addr.street": "Second", "addr.city": "Ville"},
            ]
        )
        .sort_values("id")
        .reset_index(drop=True)
    )

    assert set(expected.columns) <= set(df.columns)
    assert_frame_equal(df[expected.columns], expected, check_dtype=False)


@pytest.mark.asyncio
async def test_read_bigdata_flattens_nested(tmp_path: Path, metrics: ComponentMetrics):
    path = tmp_path / "in.jsonl"
    lines = [
        {"id": 1, "addr": {"street": "Main", "city": "Town"}},
        {"id": 2, "addr": {"street": "Second", "city": "Ville"}},
    ]
    with path.open("w", encoding="utf-8") as f:
        for r in lines:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    r = JSONReceiver()
    ddf = await r.read_bigdata(filepath=path, metrics=metrics)
    assert isinstance(ddf, dd.DataFrame)
    df = ddf.compute().sort_values("id").reset_index(drop=True)

    assert {"id", "addr.street", "addr.city"} <= set(df.columns)
    assert list(df["addr.street"]) == ["Main", "Second"]


@pytest.mark.asyncio
async def test_write_row_nested_only_and_append(
    tmp_path: Path, metrics: ComponentMetrics
):
    path = tmp_path / "rows.json"
    r = JSONReceiver()

    await r.write_row(
        filepath=path, metrics=metrics, row={"id": 1, "addr": {"street": "X"}}
    )
    await r.write_row(
        filepath=path, metrics=metrics, row={"id": 2, "addr": {"street": "Y"}}
    )

    data = json.loads(path.read_text(encoding="utf-8"))
    assert data == [
        {"id": 1, "addr": {"street": "X"}},
        {"id": 2, "addr": {"street": "Y"}},
    ]


@pytest.mark.asyncio
async def test_write_bulk_unflattens_and_readback(
    tmp_path: Path, metrics: ComponentMetrics
):
    path = tmp_path / "bulk.json"
    r = JSONReceiver()

    df_in = pd.DataFrame(
        [
            {"id": 10, "addr.street": "A", "addr.city": "AA"},
            {"id": 11, "addr.street": "B", "addr.city": "BB"},
        ]
    )
    await r.write_bulk(filepath=path, metrics=metrics, data=df_in)

    on_disk = json.loads(path.read_text(encoding="utf-8"))
    assert on_disk == [
        {"id": 10, "addr": {"street": "A", "city": "AA"}},
        {"id": 11, "addr": {"street": "B", "city": "BB"}},
    ]

    df_out = await r.read_bulk(
        filepath=path,
        metrics=ComponentMetrics(
            started_at=datetime.now(),
            processing_time=timedelta(0),
            error_count=0,
            lines_received=0,
            lines_forwarded=0,
        ),
    )
    df_out = df_out.sort_values("id").reset_index(drop=True)
    expected_flat = df_in.sort_values("id").reset_index(drop=True)
    assert set(expected_flat.columns) <= set(df_out.columns)
    assert_frame_equal(df_out[expected_flat.columns], expected_flat, check_dtype=False)


@pytest.mark.asyncio
async def test_write_bigdata_partitioned_nested(
    tmp_path: Path, metrics: ComponentMetrics
):
    path = tmp_path / "big.json"
    r = JSONReceiver()

    pdf = pd.DataFrame(
        [
            {"id": 20, "addr.street": "S1", "addr.city": "C1"},
            {"id": 21, "addr.street": "S2", "addr.city": "C2"},
        ]
    )
    ddf = dd.from_pandas(pdf, npartitions=2)

    await r.write_bigdata(filepath=path, metrics=metrics, data=ddf)

    out_dir = path.parent / f"{path.stem}_parts"
    parts = sorted(out_dir.glob("part-*.jsonl*"))
    assert parts, "no NDJSON part files written"

    rows = []
    for p in parts:
        for line in p.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if s:
                rows.append(json.loads(s))

    assert rows == [
        {"id": 20, "addr": {"street": "S1", "city": "C1"}},
        {"id": 21, "addr": {"street": "S2", "city": "C2"}},
    ]
