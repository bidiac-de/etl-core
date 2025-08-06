import pytest
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from src.components.file_components.csv.read_csv import ReadCSV
from src.components.column_definition import ColumnDefinition, DataType
from src.components.file_components.csv.csv_component import Delimiter
from src.metrics.component_metrics import ComponentMetrics
from src.strategies.row_strategy import RowExecutionStrategy
from src.strategies.bulk_strategy import BulkExecutionStrategy

DATA_DIR = Path(__file__).parent / "data/csv"
VALID_CSV = DATA_DIR / "test_data.csv"
MISSING_VALUES_CSV = DATA_DIR / "test_data_missing_values.csv"
SCHEMA_MISMATCH_CSV = DATA_DIR / "test_data_schema.csv"
WRONG_TYPES_CSV = DATA_DIR / "test_data_wrong_types.csv"
INVALID_CSV_FILE = DATA_DIR / "test_invalid_csv.csv"

@pytest.fixture(autouse=True)
def patch_strategies(monkeypatch):
    def row_exec(self, component, inputs, **kwargs):
        return component.process_row(inputs, metrics=kwargs.get("metrics"))

    def bulk_exec(self, component, inputs, **kwargs):
        return component.process_bulk(inputs, metrics=kwargs.get("metrics"))

    monkeypatch.setattr(RowExecutionStrategy, "execute", row_exec)
    monkeypatch.setattr(BulkExecutionStrategy, "execute", bulk_exec)

@pytest.fixture
def schema_definition():
    return [
        ColumnDefinition(name="id", data_type=DataType.STRING),
        ColumnDefinition(name="name", data_type=DataType.STRING),
    ]

@pytest.fixture
def metrics():
    return ComponentMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
    )


def test_readcsv_valid_bulk(schema_definition, metrics):
    comp = ReadCSV(
        name="ReadCSV_Bulk_Valid",
        description="Valid CSV file",
        comp_type="read_csv",
        filepath=VALID_CSV,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        strategy_type="bulk",
    )
    result = comp.execute(data=None, metrics=metrics)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 3
    assert set(result.columns) == {"id", "name"}


def test_readcsv_missing_values_bulk(schema_definition, metrics):
    comp = ReadCSV(
        name="ReadCSV_Bulk_Missing",
        description="Missing values",
        comp_type="read_csv",
        filepath=MISSING_VALUES_CSV,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        strategy_type="bulk",
    )
    result = comp.execute(data=None, metrics=metrics)
    assert isinstance(result, pd.DataFrame)
    assert result.isna().any().any()


def test_readcsv_schema_mismatch_bulk(schema_definition, metrics):
    comp = ReadCSV(
        name="ReadCSV_Bulk_Schema",
        description="Schema mismatch",
        comp_type="read_csv",
        filepath=SCHEMA_MISMATCH_CSV,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        strategy_type="bulk",
    )
    result = comp.execute(data=None, metrics=metrics)
    assert isinstance(result, pd.DataFrame)
    expected_cols = {col.name for col in schema_definition}
    actual_cols = set(result.columns)
    assert expected_cols != actual_cols


def test_readcsv_wrong_types_bulk(schema_definition, metrics):
    comp = ReadCSV(
        name="ReadCSV_Bulk_WrongTypes",
        description="Wrong data types",
        comp_type="read_csv",
        filepath=WRONG_TYPES_CSV,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        strategy_type="bulk",
    )
    result = comp.execute(data=None, metrics=metrics)
    assert isinstance(result, pd.DataFrame)
    assert result["id"].dtype == object



def test_readcsv_invalid_csv_content_raises(schema_definition, metrics):
    with pytest.raises(Exception):
        comp = ReadCSV(
            name="ReadCSV_Invalid_Content",
            description="File is not valid CSV format",
            comp_type="read_csv",
            filepath=INVALID_CSV_FILE,
            schema_definition=schema_definition,
            separator=Delimiter.COMMA,
            strategy_type="bulk",
        )
        comp.execute(data=None, metrics=metrics)