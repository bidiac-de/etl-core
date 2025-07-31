import pytest
import pandas as pd
from datetime import datetime
from pathlib import Path

from src.receivers.files.csv_receiver import CSVReceiver
from src.metrics.component_metrics import ComponentMetrics


@pytest.fixture
def sample_metrics():
    """Creates an empty ComponentMetrics object."""
    return ComponentMetrics(started_at=datetime.now(), processing_time=0)


@pytest.fixture
def sample_csv_file(tmp_path: Path):
    path = tmp_path / "test.csv"
    with open(path, "w", encoding="utf-8") as f:
        f.write("id,name,age\n1,Alice,30\n2,Bob,25\n")
    return path


def test_read_row_reads_first_line(sample_csv_file, sample_metrics):
    receiver = CSVReceiver()
    row = receiver.read_row(metrics=sample_metrics, filepath=sample_csv_file)
    assert row == {"id": "1", "name": "Alice", "age": "30"}


def test_read_bulk_reads_all_lines(sample_csv_file, sample_metrics):
    receiver = CSVReceiver()
    rows = receiver.read_bulk(metrics=sample_metrics, filepath=sample_csv_file)
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice"
    assert rows[1]["age"] == "25"


def test_read_bigdata_yields_rows(sample_csv_file, sample_metrics):
    receiver = CSVReceiver()
    rows = list(receiver.read_bigdata(metrics=sample_metrics, filepath=sample_csv_file))
    assert len(rows) == 2
    assert rows[0]["id"] == 1 or rows[0]["id"] == "1"  # Je nach Parser int oder str


def test_write_row_appends_line(tmp_path, sample_metrics):
    out_file = tmp_path / "out.csv"
    receiver = CSVReceiver()
    receiver.write_row(row={"id": "1", "name": "Alice", "age": "30"}, filepath=out_file, metrics=sample_metrics)
    receiver.write_row(row={"id": "2", "name": "Bob", "age": "25"}, filepath=out_file, metrics=sample_metrics)
    rows = receiver.read_bulk(metrics=sample_metrics, filepath=out_file)
    assert len(rows) == 2
    assert rows[1]["name"] == "Bob"


def test_write_bulk_overwrites_file(tmp_path, sample_metrics):
    out_file = tmp_path / "bulk.csv"
    receiver = CSVReceiver()
    data = [
        {"id": "1", "name": "Alice", "age": "30"},
        {"id": "2", "name": "Bob", "age": "25"}
    ]
    receiver.write_bulk(metrics=sample_metrics, data=data, filepath=out_file)
    rows = receiver.read_bulk(metrics=sample_metrics, filepath=out_file)
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice"


def test_write_bigdata_with_pandas(tmp_path, sample_metrics):
    out_file = tmp_path / "big.csv"
    receiver = CSVReceiver()
    df = pd.DataFrame([
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25}
    ])
    receiver.write_bigdata(metrics=sample_metrics, data=df, filepath=out_file)
    rows = receiver.read_bulk(metrics=sample_metrics, filepath=out_file)
    assert len(rows) == 2
    assert rows[1]["name"] == "Bob"