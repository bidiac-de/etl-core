import pytest
import csv
from pathlib import Path
from typing import List, Dict, Any, Generator
from src.components.data_operations.csv.read_csv import ReadCSV
from src.components.data_operations.csv.write_csv import WriteCSV
from src.components.data_operations.csv.csv_component import Delimiter



@pytest.fixture
def sample_csv_data() -> List[Dict[str, Any]]:
    return [
        {"id": "1", "name": "Alice"},
        {"id": "2", "name": "Bob"},
        {"id": "3", "name": "Charlie"}
    ]

@pytest.fixture
def sample_csv_file(tmp_path: Path, sample_csv_data: List[Dict[str, Any]]) -> Path:
    path = tmp_path / "test.csv"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name"])
        writer.writeheader()
        writer.writerows(sample_csv_data)
    return path


def test_readcsv_row(sample_csv_file):
    reader = ReadCSV(id=1, name="readRow", description="desc", componentManager=None, filepath=sample_csv_file, separator=Delimiter.COMMA)
    row = reader.process_row()
    assert isinstance(row, dict)
    assert row["name"] == "Alice"

def test_readcsv_bulk(sample_csv_file):
    reader = ReadCSV(id=2, name="readBulk", description="desc", componentManager=None, filepath=sample_csv_file, separator=Delimiter.COMMA)
    rows = reader.process_bulk()
    assert len(rows) == 3
    assert rows[1]["name"] == "Bob"

def test_readcsv_bigdata(sample_csv_file):
    reader = ReadCSV(id=3, name="readBig", description="desc", componentManager=None, filepath=sample_csv_file, separator=Delimiter.COMMA)
    generator = reader.process_bigdata()
    collected = list(generator)
    assert len(collected) == 3
    assert collected[2]["name"] == "Charlie"


def test_writecsv_row(tmp_path: Path):
    path = tmp_path / "write_row.csv"
    writer = WriteCSV(id=4, name="writeRow", description="desc", componentManager=None, filepath=path, fieldnames=["id", "name"], separator=Delimiter.COMMA)
    writer.process_row({"id": "10", "name": "Daisy"})

    with open(path, newline="") as f:
        reader = list(csv.DictReader(f))
        assert reader[0]["name"] == "Daisy"

def test_writecsv_bulk(tmp_path: Path):
    path = tmp_path / "write_bulk.csv"
    data = [{"id": "20", "name": "Eva"}, {"id": "21", "name": "Finn"}]
    writer = WriteCSV(id=5, name="writeBulk", description="desc", componentManager=None, filepath=path, fieldnames=["id", "name"], separator=Delimiter.COMMA)
    writer.process_bulk(data)

    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
        assert len(rows) == 2
        assert rows[1]["name"] == "Finn"

def test_writecsv_bigdata(tmp_path: Path):
    path = tmp_path / "write_big.csv"
    def gen_data() -> Generator[Dict[str, Any], None, None]:
        yield {"id": "30", "name": "Gina"}
        yield {"id": "31", "name": "Hugo"}

    writer = WriteCSV(id=6, name="writeBig", description="desc", componentManager=None, filepath=path, fieldnames=["id", "name"], separator=Delimiter.COMMA)
    writer.process_bigdata(gen_data())

    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
        assert len(rows) == 2
        assert rows[0]["name"] == "Gina"
        assert rows[1]["id"] == "31"