import json
import gzip
from datetime import datetime, timedelta
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from src.receivers.files.json_receiver import JSONReceiver
from src.metrics.component_metrics import ComponentMetrics


@pytest.fixture
def metrics() -> ComponentMetrics:
    return ComponentMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
    )


def test_jsonreceiver_read_row_gz(tmp_path: Path, metrics: ComponentMetrics):
    """read_row should support .gz via helper (open_text_auto)."""
    receiver = JSONReceiver()
    file_path = tmp_path / "row.json.gz"
    payload = [{"id": 1, "name": "Alice"}]

    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        json.dump(payload, f)

    assert file_path.exists()

    row = receiver.read_row(file_path, metrics=metrics)
    assert isinstance(row, dict)
    assert row["id"] == 1 and row["name"] == "Alice"


def test_jsonreceiver_row_roundtrip(tmp_path: Path, metrics: ComponentMetrics):
    """write_row → read_row roundtrip."""
    receiver = JSONReceiver()
    file_path = tmp_path / "row.json"
    row_in = {"id": 2, "name": "Bob"}

    receiver.write_row(row_in, file_path, metrics=metrics)
    assert file_path.exists()

    row_out = receiver.read_row(file_path, metrics=metrics)
    assert row_out == row_in


def test_jsonreceiver_bulk_array_json(tmp_path: Path, metrics: ComponentMetrics):
    """read_bulk for array-of-records JSON."""
    receiver = JSONReceiver()
    file_path = tmp_path / "bulk.json"
    df_in = pd.DataFrame([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])

    receiver.write_bulk(df_in, file_path, metrics=metrics)
    assert file_path.exists()

    df_out = receiver.read_bulk(file_path, metrics=metrics)
    assert isinstance(df_out, pd.DataFrame)
    assert len(df_out) == 2
    assert set(df_out.columns) >= {"id", "name"}
    assert df_out.iloc[0]["name"] in {"Alice", "Bob"}  # order not strictly guaranteed


def test_jsonreceiver_bulk_ndjson(tmp_path: Path, metrics: ComponentMetrics):
    """read_bulk should handle NDJSON if extension is .jsonl/.ndjson."""
    receiver = JSONReceiver()
    file_path = tmp_path / "data.jsonl"

    # write simple NDJSON (one JSON per line)
    lines = [
        {"id": 10, "name": "Charlie"},
        {"id": 11, "name": "Diana"},
    ]
    file_path.write_text("\n".join(json.dumps(x) for x in lines), encoding="utf-8")

    df = receiver.read_bulk(file_path, metrics=metrics)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert set(df["name"]) == {"Charlie", "Diana"}


def test_jsonreceiver_write_bigdata_and_readback_with_dask(tmp_path: Path, metrics: ComponentMetrics):
    """write_bigdata creates partitioned JSON Lines; verify by reading with dask."""
    receiver = JSONReceiver()
    out_dir = tmp_path / "bigdata_output"
    out_dir.mkdir()

    ddf_in = dd.from_pandas(
        pd.DataFrame([{"id": 100, "name": "Eve"}, {"id": 101, "name": "Frank"}]),
        npartitions=1,
    )

    receiver.write_bigdata(ddf_in, out_dir, metrics=metrics)
    parts = list(out_dir.glob("part-*.json"))
    assert parts, "No partition files written."

    ddf_out = dd.read_json([str(p) for p in parts], lines=True, blocksize="64MB")
    df_out = ddf_out.compute()
    assert len(df_out) == 2
    assert set(df_out["name"]) == {"Eve", "Frank"}


def test_jsonreceiver_read_bigdata_on_jsonl(tmp_path: Path, metrics: ComponentMetrics):
    """read_bigdata should read NDJSON when path endswith .jsonl/.ndjson."""
    receiver = JSONReceiver()
    file_path = tmp_path / "stream.jsonl"

    # two lines of NDJSON
    rows = [{"id": 200, "name": "Gina"}, {"id": 201, "name": "Hank"}]
    file_path.write_text("\n".join(json.dumps(x) for x in rows), encoding="utf-8")

    ddf = receiver.read_bigdata(file_path, metrics=metrics)
    assert isinstance(ddf, dd.DataFrame)
    df = ddf.compute()
    assert len(df) == 2
    assert set(df["name"]) == {"Gina", "Hank"}
