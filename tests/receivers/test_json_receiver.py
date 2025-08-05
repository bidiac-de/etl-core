import json
import gzip
from datetime import datetime, timedelta
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from src.receivers.files.json_receiver import JSONReceiver
from src.metrics.component_metrics import ComponentMetrics


DATA_DIR = Path(__file__).resolve().parents[1] / "components" / "data"
PEOPLE_JSON = DATA_DIR / "testdata.json"
PEOPLE_JSONL = DATA_DIR / "testdata.jsonl"


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


def test_jsonreceiver_bulk_array_json(metrics: ComponentMetrics):
    """read_bulk for array-of-records JSON from central file."""
    receiver = JSONReceiver()
    assert PEOPLE_JSON.exists(), f"Missing test data file: {PEOPLE_JSON}"

    df_out = receiver.read_bulk(PEOPLE_JSON, metrics=metrics)
    assert isinstance(df_out, pd.DataFrame)
    assert len(df_out) == 3
    assert list(df_out.sort_values("id")["name"]) == ["Alice", "Bob", "Charlie"]


def test_jsonreceiver_bulk_ndjson(metrics: ComponentMetrics):
    """read_bulk should handle NDJSON (.jsonl) from central file."""
    receiver = JSONReceiver()
    assert PEOPLE_JSONL.exists(), f"Missing test data file: {PEOPLE_JSONL}"

    df = receiver.read_bulk(PEOPLE_JSONL, metrics=metrics)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert set(df["name"]) == {"Alice", "Bob", "Charlie"}


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


def test_jsonreceiver_read_bigdata_on_jsonl(metrics: ComponentMetrics):
    """read_bigdata should read NDJSON when path endswith .jsonl/.ndjson (central file)."""
    receiver = JSONReceiver()
    assert PEOPLE_JSONL.exists(), f"Missing test data file: {PEOPLE_JSONL}"

    ddf = receiver.read_bigdata(PEOPLE_JSONL, metrics=metrics)
    assert isinstance(ddf, dd.DataFrame)
    df = ddf.compute()
    assert len(df) == 3
    assert set(df["name"]) == {"Alice", "Bob", "Charlie"}

