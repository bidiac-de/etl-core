from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import pytest

from src.components.file_components.excel.read_excel import ReadExcel
from src.components.file_components.excel.write_excel import WriteExcel
from src.components.column_definition import ColumnDefinition, DataType
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.components import Schema
from src.strategies.row_strategy import RowExecutionStrategy
from src.strategies.bulk_strategy import BulkExecutionStrategy

DATA_DIR = Path(__file__).resolve().parent / "data"
VALID_FILE = DATA_DIR / "test_data.xlsx"
MISSING_FILE = DATA_DIR / "test_data_missing.xlsx"
TYPES_FILE = DATA_DIR / "test_data_types.xlsx"
SCHEMA_FILE = DATA_DIR / "test_data_schema.xlsx"


@pytest.fixture(autouse=True)
def patch_strategies(monkeypatch):
    def row_exec(self, component, inputs, **kwargs):
        return component.process_row(inputs, metrics=kwargs.get("metrics"))

    def bulk_exec(self, component, inputs, **kwargs):
        return component.process_bulk(inputs, metrics=kwargs.get("metrics"))

    monkeypatch.setattr(RowExecutionStrategy, "execute", row_exec, raising=True)
    monkeypatch.setattr(BulkExecutionStrategy, "execute", bulk_exec, raising=True)


@pytest.fixture
def metrics():
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
        ColumnDefinition(name="id", data_type=DataType.INTEGER),
        ColumnDefinition(name="name", data_type=DataType.STRING),
    ]


@pytest.fixture
def schema(schema_definition):
    return Schema(columns=schema_definition)


def test_read_excel_row(schema, schema_definition, metrics):
    comp = ReadExcel(
        name="Read row",
        description="Read first row from Excel",
        comp_type="read_excel",
        filepath=VALID_FILE,
        strategy_type="row",
        schema=schema,
        schema_definition=schema_definition,
    )

    result = comp.execute(data={}, metrics=metrics)
    assert isinstance(result, dict)
    assert result["id"] == 1
    assert result["name"] == "Alice"


def test_read_excel_bulk_valid(schema, schema_definition, metrics):
    comp = ReadExcel(
        name="Read bulk",
        description="Read all rows",
        comp_type="read_excel",
        filepath=VALID_FILE,
        strategy_type="bulk",
        schema=schema,
        schema_definition=schema_definition,
    )

    df = comp.execute(data=None, metrics=metrics)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert list(df.sort_values("id")["name"]) == ["Alice", "Bob", "Charlie"]


def test_write_excel_bulk(tmp_path, schema, schema_definition, metrics):
    out_fp = tmp_path / "out.xlsx"
    out_fp.touch()

    writer = WriteExcel(
        name="Write bulk",
        description="Write Excel test",
        comp_type="write_excel",
        filepath=out_fp,
        strategy_type="bulk",
        schema=schema,
        schema_definition=schema_definition,
    )

    test_data = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ]
    writer.execute(data=test_data, metrics=metrics)

    df = pd.read_excel(out_fp)
    assert len(df) == 3
    assert df.iloc[2]["name"] == "Charlie"


def test_read_excel_missing_values(schema, schema_definition, metrics):
    comp = ReadExcel(
        name="Read missing",
        description="Test missing values",
        comp_type="read_excel",
        filepath=MISSING_FILE,
        strategy_type="bulk",
        schema=schema,
        schema_definition=schema_definition,
    )

    df = comp.execute(data=None, metrics=metrics)
    assert isinstance(df, pd.DataFrame)
    assert "name" in df.columns
    assert df["name"].isnull().any()


def test_read_excel_wrong_types(schema, schema_definition, metrics):
    comp = ReadExcel(
        name="Read wrong types",
        description="Test wrong data types",
        comp_type="read_excel",
        filepath=TYPES_FILE,
        strategy_type="bulk",
        schema=schema,
        schema_definition=schema_definition,
    )

    df = comp.execute(data=None, metrics=metrics)
    assert isinstance(df, pd.DataFrame)
    assert "id" in df.columns
    with pytest.raises(Exception):
        pd.to_numeric(df["id"], errors="raise")


def test_read_excel_schema_mismatch(schema, schema_definition, metrics):
    comp = ReadExcel(
        name="Read schema mismatch",
        description="Schema mismatch test",
        comp_type="read_excel",
        filepath=SCHEMA_FILE,
        strategy_type="bulk",
        schema=schema,
        schema_definition=schema_definition,
    )

    df = comp.execute(data=None, metrics=metrics)
    assert isinstance(df, pd.DataFrame)

    assert set(df.columns) != {"id", "name"}