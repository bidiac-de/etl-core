import json

import pandas as pd
import pytest
from pathlib import Path
from components.file_components.json.read_json_component import ReadJSON
from src.components.file_components.json.write_json_component import WriteJSON
from src.components.column_definition import ColumnDefinition, DataType
from datetime import datetime


@pytest.fixture
def sample_json_file(tmp_path: Path):
    """Creates a sample JSON file for testing."""
    file_path = tmp_path / "test.json"
    data = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"}
    ]
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return file_path


@pytest.fixture
def schema_definition():
    """Sample schema using ColumnDefinition."""
    return [
        ColumnDefinition("id", DataType.INTEGER),
        ColumnDefinition("name", DataType.STRING)
    ]


def test_readjson_bulk(sample_json_file, schema_definition):
    """Tests reading all data using ReadJSON."""
    reader = ReadJSON(
        id=1,
        name="ReadTest",
        description="Test ReadJSON",
        componentManager=None,
        filepath=sample_json_file,
        schema_definition=schema_definition,
        created_by=2,
        created_at=datetime.now()
    )
    data = reader.process_bulk([])
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 2
    records = data.to_dict(orient="records")
    assert records[0]["name"] == "Alice"

def test_readjson_row(sample_json_file, schema_definition):
    """Tests reading a single row using ReadJSON."""
    reader = ReadJSON(
        id=1,
        name="ReadTest",
        description="Test ReadJSON",
        componentManager=None,
        filepath=sample_json_file,
        schema_definition=schema_definition,
        created_by=2,
        created_at=datetime.now()
    )
    row = reader.process_row({})
    assert isinstance(row, dict)
    assert "name" in row


def test_writejson_and_readback(tmp_path, schema_definition):
    """Tests writing data with WriteJSON and reading it back with ReadJSON."""
    file_path = tmp_path / "output.json"
    writer = WriteJSON(
        id=1,
        name="WriteTest",
        description="Test WriteJSON",
        componentManager=None,
        filepath=file_path,
        schema_definition=schema_definition,
        created_by=2,
        created_at=datetime.now()
    )

    import pandas as pd  # falls noch nicht oben

    test_data = pd.DataFrame([
        {"id": 10, "name": "Charlie"},
        {"id": 11, "name": "Diana"}
    ])

    writer.process_bulk(test_data)

    # Read and verify
    reader = ReadJSON(
        id=2,
        name="ReadTest",
        description="Test ReadJSON",
        componentManager=None,
        filepath=file_path,
        schema_definition=schema_definition,
        created_by=2,
        created_at=datetime.now()
    )
    read_data = reader.process_bulk([])
    assert len(read_data) == 2
    assert read_data.iloc[0]["name"] == "Charlie"


def test_writejson_row(tmp_path, schema_definition):
    """Tests writing a single row with WriteJSON."""
    file_path = tmp_path / "row.json"
    writer = WriteJSON(
        id=1,
        name="WriteRowTest",
        description="Test Write Row",
        componentManager=None,
        filepath=file_path,
        schema_definition=schema_definition,
        created_by=2,
        created_at=datetime.now()
    )
    row = {"id": 99, "name": "SingleRow"}
    writer.process_row(row)

    with open(file_path, "r", encoding="utf-8") as f:
        content = json.load(f)
    assert content[0]["id"] == 99
    assert content[0]["name"] == "SingleRow"