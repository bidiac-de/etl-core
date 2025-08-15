import pytest
import pandas as pd
import dask.dataframe as dd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, AsyncGenerator, AsyncIterator

from src.components.file_components.csv.read_csv import ReadCSV
from src.components.file_components.csv.write_csv import WriteCSV
from src.components.column_definition import ColumnDefinition, DataType
from src.components.file_components.csv.csv_component import Delimiter
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.strategies.row_strategy import RowExecutionStrategy
from src.strategies.bulk_strategy import BulkExecutionStrategy
from src.strategies.bigdata_strategy import BigDataExecutionStrategy
from src.components import Schema

DATA_DIR = Path(__file__).parent / "data/csv"
VALID_CSV = DATA_DIR / "test_data.csv"
MISSING_VALUES_CSV = DATA_DIR / "test_data_missing_values.csv"
SCHEMA_MISMATCH_CSV = DATA_DIR / "test_data_schema.csv"
WRONG_TYPES_CSV = DATA_DIR / "test_data_wrong_types.csv"
INVALID_CSV_FILE = DATA_DIR / "test_invalid_csv.csv"


def build_minimal_schema() -> Schema:
    return Schema(
        columns=[
            ColumnDefinition(name="id", data_type=DataType.STRING),
            ColumnDefinition(name="name", data_type=DataType.STRING),
        ]
    )


@pytest.fixture(autouse=True)
def patch_strategies(monkeypatch):
    async def _collect_or_return(x):
        if hasattr(x, "__aiter__"):
            items = []
            async for item in x:
                items.append(item)
            return items
        if hasattr(x, "__await__"):
            return await x
        return x

    async def _call_with_fallback(fn, payload, metrics):
        try:
            return await _collect_or_return(fn(payload, metrics=metrics))
        except TypeError as e:
            if "multiple values for argument 'metrics'" not in str(e):
                raise
            return await _collect_or_return(fn(metrics, payload))

    async def row_exec(self, component, payload, metrics):
        return await _call_with_fallback(component.process_row, payload, metrics)

    async def bulk_exec(self, component, payload, metrics):
        return await _call_with_fallback(component.process_bulk, payload, metrics)

    async def bigdata_exec(self, component, payload, metrics):
        return await _call_with_fallback(component.process_bigdata, payload, metrics)

    from src.strategies.row_strategy import RowExecutionStrategy
    from src.strategies.bulk_strategy import BulkExecutionStrategy
    from src.strategies.bigdata_strategy import BigDataExecutionStrategy

    monkeypatch.setattr(RowExecutionStrategy, "execute", row_exec, raising=True)
    monkeypatch.setattr(BulkExecutionStrategy, "execute", bulk_exec, raising=True)
    monkeypatch.setattr(BigDataExecutionStrategy, "execute", bigdata_exec, raising=True)


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


@pytest.mark.asyncio
async def test_readcsv_valid_bulk(schema_definition, metrics):
    comp = ReadCSV(
        name="ReadCSV_Bulk_Valid",
        description="Valid CSV file",
        comp_type="read_csv",
        filepath=VALID_CSV,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        schema=build_minimal_schema(),
    )
    comp.strategy = BulkExecutionStrategy()

    res = await comp.execute(payload=None, metrics=metrics)
    df = res[0] if isinstance(res, list) else res
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert set(df.columns) == {"id", "name"}


@pytest.mark.asyncio
async def test_readcsv_missing_values_bulk(schema_definition, metrics):
    comp = ReadCSV(
        name="ReadCSV_Bulk_Missing",
        description="Missing values",
        comp_type="read_csv",
        filepath=MISSING_VALUES_CSV,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        schema=build_minimal_schema(),
    )
    comp.strategy = BulkExecutionStrategy()

    res = await comp.execute(payload=None, metrics=metrics)
    df = res[0] if isinstance(res, list) else res
    assert isinstance(df, pd.DataFrame)
    assert df.isna().any().any()


@pytest.mark.asyncio
async def test_readcsv_schema_mismatch_bulk(schema_definition, metrics):
    comp = ReadCSV(
        name="ReadCSV_Bulk_Schema",
        description="Schema mismatch",
        comp_type="read_csv",
        filepath=SCHEMA_MISMATCH_CSV,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        schema=build_minimal_schema(),
    )
    comp.strategy = BulkExecutionStrategy()

    res = await comp.execute(payload=None, metrics=metrics)
    df = res[0] if isinstance(res, list) else res
    assert isinstance(df, pd.DataFrame)
    expected_cols = {col.name for col in schema_definition}
    actual_cols = set(df.columns)
    assert expected_cols != actual_cols


@pytest.mark.asyncio
async def test_readcsv_wrong_types_bulk(schema_definition, metrics):
    comp = ReadCSV(
        name="ReadCSV_Bulk_WrongTypes",
        description="Wrong data types",
        comp_type="read_csv",
        filepath=WRONG_TYPES_CSV,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        schema=build_minimal_schema(),
    )
    comp.strategy = BulkExecutionStrategy()

    res = await comp.execute(payload=None, metrics=metrics)
    df = res[0] if isinstance(res, list) else res
    assert isinstance(df, pd.DataFrame)
    assert df["id"].dtype == object


@pytest.mark.asyncio
async def test_readcsv_invalid_csv_content_raises(schema_definition, metrics):
    comp = ReadCSV(
        name="ReadCSV_Invalid_Content",
        description="File is not valid CSV format",
        comp_type="read_csv",
        filepath=INVALID_CSV_FILE,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        schema=build_minimal_schema(),
    )
    comp.strategy = BulkExecutionStrategy()

    with pytest.raises(Exception):
        _ = await comp.execute(payload=None, metrics=metrics)


@pytest.mark.asyncio
async def test_readcsv_row_streaming(schema_definition, metrics):
    comp = ReadCSV(
        name="ReadCSV_Row_Stream",
        description="Row streaming",
        comp_type="read_csv",
        filepath=VALID_CSV,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        schema=build_minimal_schema(),
    )
    comp.strategy = RowExecutionStrategy()

    rows: List[Dict[str, Any]] = await comp.execute(payload=None, metrics=metrics)
    assert isinstance(rows, list)
    assert len(rows) == 3
    assert set(rows[0].keys()) == {"id", "name"}


@pytest.mark.asyncio
async def test_readcsv_bigdata(schema_definition, metrics):
    comp = ReadCSV(
        name="ReadCSV_BigData",
        description="Read CSV with Dask",
        comp_type="read_csv",
        filepath=VALID_CSV,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        schema=build_minimal_schema(),
    )
    comp.strategy = BigDataExecutionStrategy()

    res = await comp.execute(payload=None, metrics=metrics)
    ddf_res = res[0] if isinstance(res, list) else res
    assert isinstance(ddf_res, dd.DataFrame)
    df = ddf_res.compute().sort_values("id")
    assert list(df["name"]) == ["Alice", "Bob", "Charlie"]


@pytest.mark.asyncio
async def test_writecsv_row(tmp_path: Path, schema_definition, metrics):
    out_fp = tmp_path / "single.csv"

    comp = WriteCSV(
        name="WriteCSV_Row",
        description="Write single row",
        comp_type="write_csv",
        filepath=out_fp,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        schema=build_minimal_schema(),
    )
    comp.strategy = RowExecutionStrategy()

    row = {"id": "1", "name": "Zoe"}
    result = await comp.execute(payload=row, metrics=metrics)
    if isinstance(result, list):
        assert len(result) == 1
        result = result[0]
    assert result == row

    reader = ReadCSV(
        name="ReadBack_Row",
        description="Read back written single",
        comp_type="read_csv",
        filepath=out_fp,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        schema=build_minimal_schema(),
    )
    reader.strategy = BulkExecutionStrategy()
    res = await reader.execute(payload=None, metrics=metrics)
    df = res[0] if isinstance(res, list) else res
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["name"] == "Zoe"


@pytest.mark.asyncio
async def test_writecsv_bulk(tmp_path: Path, schema_definition, metrics):
    out_fp = tmp_path / "bulk.csv"

    comp = WriteCSV(
        name="WriteCSV_Bulk",
        description="Write multiple rows",
        comp_type="write_csv",
        filepath=out_fp,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        schema=build_minimal_schema(),
    )
    comp.strategy = BulkExecutionStrategy()

    data = [
        {"id": "1", "name": "A"},
        {"id": "2", "name": "B"},
        {"id": "3", "name": "C"},
    ]

    res = await comp.execute(payload=data, metrics=metrics)
    if isinstance(res, list) and len(res) == 1 and isinstance(res[0], list):
        res = res[0]

    assert isinstance(res, list) and len(res) == 3
    assert out_fp.exists()

    reader = ReadCSV(
        name="ReadBack_Bulk",
        description="Read back bulk",
        comp_type="read_csv",
        filepath=out_fp,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        schema=build_minimal_schema(),
    )
    reader.strategy = BulkExecutionStrategy()
    df = await reader.execute(payload=None, metrics=metrics)
    if isinstance(df, list):
        df = df[0]
    assert list(df.sort_values("id")["name"]) == ["A", "B", "C"]


@pytest.mark.asyncio
async def test_writecsv_bigdata(tmp_path: Path, schema_definition, metrics):
    out_fp = tmp_path / "big.csv"

    comp = WriteCSV(
        name="WriteCSV_BigData",
        description="Write Dask DataFrame",
        comp_type="write_csv",
        filepath=out_fp,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        schema=build_minimal_schema(),
    )
    comp.strategy = BigDataExecutionStrategy()

    ddf_in = dd.from_pandas(
        pd.DataFrame(
            [
                {"id": "10", "name": "Nina"},
                {"id": "11", "name": "Omar"},
            ]
        ),
        npartitions=2,
    )

    result = await comp.execute(payload=ddf_in, metrics=metrics)
    ddf_out = result[0] if isinstance(result, list) else result
    assert isinstance(ddf_out, dd.DataFrame)
    assert out_fp.exists()

    reader = ReadCSV(
        name="ReadBack_BigData",
        description="Read back big",
        comp_type="read_csv",
        filepath=out_fp,
        schema_definition=schema_definition,
        separator=Delimiter.COMMA,
        schema=build_minimal_schema(),
    )
    reader.strategy = BulkExecutionStrategy()
    read_res = await reader.execute(payload=None, metrics=metrics)
    df = read_res[0] if isinstance(read_res, list) else read_res
    assert list(df.sort_values("id")["name"]) == ["Nina", "Omar"]
