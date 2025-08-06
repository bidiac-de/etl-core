import csv
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.receivers.files.csv_receiver import CSVReceiver
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


@pytest.fixture
def sample_csv_file() -> Path:
    return Path(__file__).parent.parent / "components" / "data" / "csv"/ "test_data.csv"


def test_readcsv_row(sample_csv_file, metrics):
    receiver = CSVReceiver(sample_csv_file)
    row = receiver.read_row(metrics=metrics, line_number=0)
    assert isinstance(row, dict)
    assert row["id"] == "1"
    assert row["name"] == "Alice"


def test_readcsv_bulk(sample_csv_file, metrics):
    receiver = CSVReceiver(sample_csv_file)
    df = receiver.read_bulk(metrics=metrics)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3  # Alice, Bob, Charlie
    assert set(df.columns) == {"id", "name"}
    assert "Bob" in df["name"].values


def test_readcsv_bigdata(sample_csv_file, metrics):
    receiver = CSVReceiver(sample_csv_file)
    rows = list(receiver.read_bigdata(metrics=metrics))
    assert len(rows) == 3
    assert any(r["name"] == "Charlie" for r in rows)


def test_writecsv_row(tmp_path: Path, metrics):
    file_path = tmp_path / "out_row.csv"
    receiver = CSVReceiver(file_path)

    receiver.write_row(metrics=metrics, row={"id": "10", "name": "Daisy"})
    receiver.write_row(metrics=metrics, row={"id": "11", "name": "Eli"})

    df = receiver.read_bulk(metrics=metrics)
    assert len(df) == 2
    assert set(df["name"]) == {"Daisy", "Eli"}


def test_writecsv_bulk(tmp_path: Path, metrics):
    file_path = tmp_path / "out_bulk.csv"
    file_path.touch()
    receiver = CSVReceiver(file_path)

    data = [
        {"id": "20", "name": "Finn"},
        {"id": "21", "name": "Gina"},
    ]
    receiver.write_bulk(metrics=metrics, data=data)

    df = receiver.read_bulk(metrics=metrics)
    assert len(df) == 2
    assert set(df["name"]) == {"Finn", "Gina"}


def test_writecsv_bigdata(tmp_path: Path, metrics):
    file_path = tmp_path / "out_big.csv"
    file_path.touch()
    receiver = CSVReceiver(file_path)

    df_in = pd.DataFrame([
        {"id": 30, "name": "Hugo"},
        {"id": 31, "name": "Ivy"},
    ])
    receiver.write_bigdata(metrics=metrics, data=df_in)

    df_out = receiver.read_bulk(metrics=metrics)
    assert len(df_out) == 2
    assert set(df_out["name"]) == {"Hugo", "Ivy"}