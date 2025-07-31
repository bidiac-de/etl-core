import pytest
import csv
from pathlib import Path
from typing import List, Dict, Any, Generator
from datetime import datetime

from src.components.file_components.csv.read_csv import ReadCSV
from src.components.file_components.csv.write_csv import WriteCSV
from src.components.file_components.csv.csv_component import Delimiter
from src.components.column_definition import ColumnDefinition, DataType
from src.metrics.component_metrics import ComponentMetrics

@pytest.fixture
def schema_def() -> List[ColumnDefinition]:
    return [
        ColumnDefinition(name="id", data_type=DataType.STRING),
        ColumnDefinition(name="name", data_type=DataType.STRING),
    ]


@pytest.fixture
def sample_csv_data() -> List[Dict[str, Any]]:
    return [
        {"id": "1", "name": "Alice"},
        {"id": "2", "name": "Bob"},
        {"id": "3", "name": "Charlie"}
    ]


@pytest.fixture
def sample_csv_file(tmp_path: Path, sample_csv_data: List[Dict[str, Any]]) -> Path:
    """Create a temporary CSV file with sample data."""
    path = tmp_path / "test.csv"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name"])
        writer.writeheader()
        writer.writerows(sample_csv_data)
    return path


@pytest.fixture
def empty_metrics() -> ComponentMetrics:
    """Create an empty ComponentMetrics object."""
    return ComponentMetrics(started_at=datetime.now(), processing_time=0)


def test_readcsv_row(sample_csv_file, schema_def, empty_metrics):
    reader = ReadCSV(
        name="ReadCSVRow",
        description="Test reading one row",
        comp_type="csv",
        filepath=sample_csv_file,
        schema_definition=schema_def,
        separator=Delimiter.COMMA,
        strategy_type="row"
    )
    row = reader.process_row({}, empty_metrics)
    assert isinstance(row, dict)
    assert row["id"] == "1"
    assert row["name"] == "Alice"


def test_readcsv_bulk(sample_csv_file, schema_def, empty_metrics):
    reader = ReadCSV(
        name="ReadCSV_Bulk",
        description="Test reading bulk",
        comp_type="csv",
        filepath=sample_csv_file,
        schema_definition=schema_def,
        separator=Delimiter.COMMA,
        strategy_type="bulk"
    )
    rows = reader.process_bulk([], empty_metrics)
    assert len(rows) == 3
    assert rows[1]["name"] == "Bob"


def test_readcsv_bigdata(sample_csv_file, schema_def, empty_metrics):
    reader = ReadCSV(
        name="ReadCSV_BigData",
        description="Test reading with bigdata strategy",
        comp_type="csv",
        filepath=sample_csv_file,
        schema_definition=schema_def,
        separator=Delimiter.COMMA,
        strategy_type="bigdata"
    )
    rows = list(reader.process_bigdata(None, empty_metrics))
    assert len(rows) == 3
    assert rows[2]["name"] == "Charlie"


def test_writecsv_row(tmp_path: Path, schema_def, empty_metrics):
    path = tmp_path / "write_row.csv"
    writer = WriteCSV(
        name="WriteCSVRow",
        description="Test writing one row",
        comp_type="csv",
        filepath=path,
        schema_definition=schema_def,
        separator=Delimiter.COMMA,
        strategy_type="row"
    )
    writer.process_row({"id": "10", "name": "Daisy"}, empty_metrics)

    with open(path, newline="") as f:
        reader = list(csv.DictReader(f))
        assert reader[0]["id"] == "10"
        assert reader[0]["name"] == "Daisy"


def test_writecsv_bulk(tmp_path: Path, schema_def, empty_metrics):
    path = tmp_path / "write_bulk.csv"
    data = [{"id": "20", "name": "Eva"}, {"id": "21", "name": "Finn"}]

    writer = WriteCSV(
        name="WriteCSV_Bulk",
        description="Test writing bulk",
        comp_type="csv",
        filepath=path,
        schema_definition=schema_def,
        separator=Delimiter.COMMA,
        strategy_type="bulk"
    )
    writer.process_bulk(data, empty_metrics)

    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
        assert len(rows) == 2
        assert rows[1]["name"] == "Finn"


def test_writecsv_bigdata(tmp_path: Path, schema_def, empty_metrics):
    path = tmp_path / "write_bigdata.csv"

    def gen_data() -> Generator[Dict[str, Any], None, None]:
        yield {"id": "30", "name": "Gina"}
        yield {"id": "31", "name": "Hugo"}

    writer = WriteCSV(
        name="WriteCSV_BigData",
        description="Test writing big data",
        comp_type="csv",
        filepath=path,
        schema_definition=schema_def,
        separator=Delimiter.COMMA,
        strategy_type="bigdata"
    )
    writer.process_bigdata(gen_data(), empty_metrics)

    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
        assert len(rows) == 2
        assert rows[0]["name"] == "Gina"
        assert rows[1]["id"] == "31"