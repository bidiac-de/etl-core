import csv
import pytest
from pathlib import Path
from typing import List, Dict
from src.receivers.csv_receiver import CSVReceiver


@pytest.fixture
def csv_path(tmp_path) -> Path:
    return tmp_path / "test.csv"


@pytest.fixture
def sample_data() -> List[Dict[str, str]]:
    return [
        {"id": "1", "name": "Alice"},
        {"id": "2", "name": "Bob"},
        {"id": "3", "name": "Charlie"},
    ]


def test_write_row(csv_path, sample_data):
    with open(csv_path, mode='w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=sample_data[0].keys())
        writer.writeheader()

    receiver = CSVReceiver(file_path=str(csv_path))
    receiver.write_row(sample_data[0])

    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) == 1
    assert rows[0] == sample_data[0]


def test_write_bulk(csv_path, sample_data):
    receiver = CSVReceiver(file_path=str(csv_path))
    receiver.write_bulk(sample_data)

    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) == 3
    assert rows == sample_data


def test_write_bigdata(csv_path, sample_data):
    receiver = CSVReceiver(file_path=str(csv_path))
    generator = (row for row in sample_data)
    receiver.write_bigdata(generator)

    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) == 3
    assert rows == sample_data


def test_read_row(csv_path, sample_data):
    with open(csv_path, mode='w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=sample_data[0].keys())
        writer.writeheader()
        writer.writerows(sample_data)

    receiver = CSVReceiver(file_path=str(csv_path))
    row = receiver.read_row()

    assert row == sample_data[0]


def test_read_bulk(csv_path, sample_data):
    with open(csv_path, mode='w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=sample_data[0].keys())
        writer.writeheader()
        writer.writerows(sample_data)

    receiver = CSVReceiver(file_path=str(csv_path))
    rows = receiver.read_bulk()

    assert rows == sample_data


def test_read_bigdata(csv_path, sample_data):
    with open(csv_path, mode='w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=sample_data[0].keys())
        writer.writeheader()
        writer.writerows(sample_data)

    receiver = CSVReceiver(file_path=str(csv_path))
    rows = list(receiver.read_bigdata())

    assert rows == sample_data