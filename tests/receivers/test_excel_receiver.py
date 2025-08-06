import pytest
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from src.receivers.files.excel_receiver import ExcelReceiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics


@pytest.fixture
def metrics() -> ComponentMetrics:
    return ComponentMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
    )


def test_excelreceiver_row_roundtrip(tmp_path: Path, metrics: ComponentMetrics):
    """write_row → read_row roundtrip."""
    file_path = tmp_path / "row.xlsx"
    receiver = ExcelReceiver(file_path)

    row_in = {"id": 1, "name": "Alice"}
    receiver.write_row(metrics, row_in)
    assert file_path.exists()

    row_out = receiver.read_row(metrics, row_index=0)
    assert isinstance(row_out, dict)
    assert row_out == row_in


def test_excelreceiver_bulk_roundtrip(tmp_path: Path, metrics: ComponentMetrics):
    """write_bulk → read_bulk roundtrip."""
    file_path = tmp_path / "bulk.xlsx"
    receiver = ExcelReceiver(file_path)

    data_in = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ]
    receiver.write_bulk(metrics, data_in)
    assert file_path.exists()

    df_out = receiver.read_bulk(metrics).sort_values("id")
    assert isinstance(df_out, pd.DataFrame)
    assert list(df_out["name"]) == ["Alice", "Bob", "Charlie"]


def test_excelreceiver_bigdata_write_and_read(tmp_path: Path, metrics: ComponentMetrics):
    """write_bigdata with DataFrame → read_bulk."""
    file_path = tmp_path / "bigdata.xlsx"
    receiver = ExcelReceiver(file_path)

    df_in = pd.DataFrame([{"id": 100, "name": "Eve"}, {"id": 101, "name": "Frank"}])
    receiver.write_bigdata(metrics, df_in)
    assert file_path.exists()

    df_out = receiver.read_bulk(metrics)
    assert len(df_out) == 2
    assert set(df_out["name"]) == {"Eve", "Frank"}


def test_excelreceiver_read_bigdata(tmp_path: Path, metrics: ComponentMetrics):
    """read_bigdata should yield dicts row by row."""
    file_path = tmp_path / "bigdata_read.xlsx"
    receiver = ExcelReceiver(file_path)

    data = [{"id": i, "name": f"User{i}"} for i in range(5)]
    receiver.write_bulk(metrics, data)

    rows = list(receiver.read_bigdata(metrics))
    assert len(rows) == 5
    assert all(isinstance(row, dict) for row in rows)
    assert rows[0]["id"] == 0 and rows[-1]["id"] == 4