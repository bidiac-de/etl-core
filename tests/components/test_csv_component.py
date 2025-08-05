import csv
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Generator

import pandas as pd
import pytest

from src.components.file_components.csv.read_csv import ReadCSV
from src.components.file_components.csv.write_csv import WriteCSV
from src.components.file_components.csv.csv_component import Delimiter
from src.components.column_definition import ColumnDefinition, DataType
from src.metrics.component_metrics import ComponentMetrics

from src.strategies.row_strategy import RowExecutionStrategy
from src.strategies.bulk_strategy import BulkExecutionStrategy
from src.strategies.bigdata_strategy import BigDataExecutionStrategy


@pytest.fixture(autouse=True)
def patch_strategies(monkeypatch):
    """Patch strategies so execute() calls process_* methods directly."""
    def row_exec(self, component, inputs, **kwargs):
        return component.process_row(inputs, metrics=kwargs.get("metrics"))

    def bulk_exec(self, component, inputs, **kwargs):
        return component.process_bulk(inputs, metrics=kwargs.get("metrics"))

    def bigdata_exec(self, component, inputs, **kwargs):
        return component.process_bigdata(inputs, metrics=kwargs.get("metrics"))

    monkeypatch.setattr(RowExecutionStrategy, "execute", row_exec)
    monkeypatch.setattr(BulkExecutionStrategy, "execute", bulk_exec)
    monkeypatch.setattr(BigDataExecutionStrategy, "execute", bigdata_exec)


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
def schema_definition():
    return [
        ColumnDefinition(name="id", data_type=DataType.STRING),
        ColumnDefinition(name="name", data_type=DataType.STRING),
    ]


@pytest.fixture
def sample_csv_file() -> Path:
    path = Path(__file__).parent / "data" / "sample.csv"
    assert path.exists(), f"CSV-Datei nicht gefunden: {path}"
    return path


def test_readcsv_row(sample_csv_file, schema_definition, metrics):
    comp = ReadCSV(
        name="ReadCSVRow",
        description="Test reading one row",
        comp_type="read_csv",
        filepath=sample_csv_file,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        strategy_type="row",
    )
    result = comp.execute(data={}, metrics=metrics)
    assert isinstance(result, dict)
    assert result["id"] == "1"
    assert result["name"] == "Alice"


def test_readcsv_bulk(sample_csv_file, schema_definition, metrics):
    comp = ReadCSV(
        name="ReadCSV_Bulk",
        description="Test reading bulk",
        comp_type="read_csv",
        filepath=sample_csv_file,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        strategy_type="bulk",
    )
    result = comp.execute(data=None, metrics=metrics)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 3
    assert "Bob" in result["name"].values


def test_readcsv_bigdata(sample_csv_file, schema_definition, metrics):
    comp = ReadCSV(
        name="ReadCSV_BigData",
        description="Test reading with bigdata",
        comp_type="read_csv",
        filepath=sample_csv_file,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        strategy_type="bigdata",
    )
    result = comp.execute(data=None, metrics=metrics)
    rows = list(result)
    assert len(rows) == 3
    assert any(r["name"] == "Charlie" for r in rows)


def test_writecsv_row(tmp_path: Path, schema_definition, metrics, sample_csv_file):
    path = tmp_path / "write_row.csv"
    path.write_text(sample_csv_file.read_text())

    comp = WriteCSV(
        name="WriteCSVRow",
        description="Test writing one row",
        comp_type="write_csv",
        filepath=path,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        strategy_type="row",
    )
    comp.execute(data={"id": "10", "name": "Daisy"}, metrics=metrics)

    with open(path, newline="") as f:
        reader = list(csv.DictReader(f))
        assert reader[-1]["id"] == "10"
        assert reader[-1]["name"] == "Daisy"


def test_writecsv_bulk(tmp_path: Path, schema_definition, metrics, sample_csv_file):
    path = tmp_path / "write_bulk.csv"
    path.write_text(sample_csv_file.read_text())

    comp = WriteCSV(
        name="WriteCSV_Bulk",
        description="Test writing bulk",
        comp_type="write_csv",
        filepath=path,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        strategy_type="bulk",
    )
    df = pd.DataFrame([
        {"id": "20", "name": "Eva"},
        {"id": "21", "name": "Finn"},
    ])
    comp.execute(data=df.to_dict(orient="records"), metrics=metrics)

    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
        assert any(r["name"] == "Eva" for r in rows)
        assert any(r["name"] == "Finn" for r in rows)


def test_writecsv_bigdata(tmp_path: Path, schema_definition, metrics, sample_csv_file):
    path = tmp_path / "write_bigdata.csv"
    path.write_text(sample_csv_file.read_text())

    def gen_data() -> Generator[Dict[str, Any], None, None]:
        yield {"id": "30", "name": "Gina"}
        yield {"id": "31", "name": "Hugo"}

    comp = WriteCSV(
        name="WriteCSV_BigData",
        description="Test writing big data",
        comp_type="write_csv",
        filepath=path,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        strategy_type="bigdata",
    )
    comp.execute(data=gen_data(), metrics=metrics)

    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
        assert any(r["name"] == "Gina" for r in rows)
        assert any(r["id"] == "31" for r in rows)