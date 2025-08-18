import asyncio
import inspect
import pytest
import pandas as pd
import dask.dataframe as dd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, AsyncGenerator

from src.components.file_components.excel.read_excel import ReadExcel
from src.components.file_components.excel.write_excel import WriteExcel
from src.components.column_definition import ColumnDefinition, DataType
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.strategies.row_strategy import RowExecutionStrategy
from src.strategies.bulk_strategy import BulkExecutionStrategy
from src.strategies.bigdata_strategy import BigDataExecutionStrategy
from src.components import Schema

DATA_DIR = Path(__file__).parent / "data"
VALID_XLSX = DATA_DIR / "test_data.xlsx"
MISSING_VALUES_XLSX = DATA_DIR / "test_data_missing.xlsx"
SCHEMA_MISMATCH_XLSX = DATA_DIR / "test_data_schema.xlsx"
WRONG_TYPES_XLSX = DATA_DIR / "test_data_types.xlsx"


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
    return Schema(
        columns=[
            ColumnDefinition(name="id", data_type=DataType.STRING),
            ColumnDefinition(name="name", data_type=DataType.STRING),
        ]
    )


@pytest.mark.asyncio
async def test_readexcel_valid_bulk(metrics, schema_definition):
    comp = ReadExcel(
        name="ReadExcel_Bulk_Valid",
        description="Valid Excel file",
        comp_type="read_excel",
        filepath=VALID_XLSX,
        schema=schema_definition,
    )
    comp.strategy = BulkExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)
    assert isinstance(res, AsyncGenerator)

    async for item in res:
        assert isinstance(item, pd.DataFrame)
        assert {"id", "name"}.issubset(set(item.columns))


@pytest.mark.asyncio
async def test_readexcel_missing_values_bulk(metrics, schema_definition):
    comp = ReadExcel(
        name="ReadExcel_Bulk_Missing",
        description="Missing values",
        comp_type="read_excel",
        filepath=MISSING_VALUES_XLSX,
        schema=schema_definition,
    )
    comp.strategy = BulkExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)
    async for df in res:
        assert isinstance(df, pd.DataFrame)
        assert df.isna().any().any()


@pytest.mark.asyncio
async def test_readexcel_row_streaming(schema_definition, metrics):
    comp = ReadExcel(
        name="ReadExcel_Row_Stream",
        description="Row streaming",
        comp_type="read_excel",
        filepath=VALID_XLSX,
        schema=schema_definition,
    )
    comp.strategy = RowExecutionStrategy()

    rows = comp.execute(payload=None, metrics=metrics)
    assert inspect.isasyncgen(rows) or isinstance(rows, AsyncGenerator)

    first = await asyncio.wait_for(anext(rows), timeout=0.25)
    assert set(first.keys()) == {"id", "name"}

    second = await asyncio.wait_for(anext(rows), timeout=0.25)
    assert set(second.keys()) == {"id", "name"}

    await rows.aclose()


@pytest.mark.asyncio
async def test_readexcel_bigdata(schema_definition, metrics):
    comp = ReadExcel(
        name="ReadExcel_BigData",
        description="Read Excel with Dask",
        comp_type="read_excel",
        filepath=VALID_XLSX,
        schema=schema_definition,
    )
    comp.strategy = BigDataExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)
    async for ddf in res:
        assert isinstance(ddf, dd.DataFrame)
        pdf = ddf.compute()
        assert {"id", "name"}.issubset(set(pdf.columns))


@pytest.mark.asyncio
async def test_writeexcel_row(tmp_path: Path, schema_definition, metrics):
    out_fp = tmp_path / "single.xlsx"

    comp = WriteExcel(
        name="WriteExcel_Row",
        description="Write single row",
        comp_type="write_excel",
        filepath=out_fp,
        schema=schema_definition,
    )
    comp.strategy = RowExecutionStrategy()

    row = {"id": "1", "name": "Zoe"}
    await anext(comp.execute(payload=row, metrics=metrics), None)

    assert out_fp.exists()

    df = pd.read_excel(out_fp)
    assert list(df.columns) == ["id", "name"]
    assert df.iloc[0]["name"] == "Zoe"


@pytest.mark.asyncio
async def test_writeexcel_bulk(tmp_path: Path, schema_definition, metrics):
    out_fp = tmp_path / "bulk.xlsx"

    comp = WriteExcel(
        name="WriteExcel_Bulk",
        description="Write multiple rows",
        comp_type="write_excel",
        filepath=out_fp,
        schema=schema_definition,
    )
    comp.strategy = BulkExecutionStrategy()

    data = pd.DataFrame(
        [
            {"id": "1", "name": "A"},
            {"id": "2", "name": "B"},
            {"id": "3", "name": "C"},
        ]
    )

    await anext(comp.execute(payload=data, metrics=metrics), None)

    assert out_fp.exists()

    df = pd.read_excel(out_fp)
    assert list(df.sort_values("id")["name"]) == ["A", "B", "C"]


@pytest.mark.asyncio
async def test_writeexcel_bigdata(tmp_path: Path, schema_definition, metrics):
    out_fp = tmp_path / "big.xlsx"

    comp = WriteExcel(
        name="WriteExcel_BigData",
        description="Write Dask DataFrame",
        comp_type="write_excel",
        filepath=out_fp,
        schema=schema_definition,
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

    await anext(comp.execute(payload=ddf_in, metrics=metrics), None)

    assert out_fp.exists()

    df = pd.read_excel(out_fp)
    assert list(df.sort_values("id")["name"]) == ["Nina", "Omar"]


@pytest.mark.asyncio
async def test_readexcel_schema_mismatch_bulk(schema_definition, metrics):
    comp = ReadExcel(
        name="ReadExcel_Bulk_Schema",
        description="Schema mismatch",
        comp_type="read_excel",
        filepath=DATA_DIR / "test_data_schema.xlsx",
        schema=schema_definition,
    )
    comp.strategy = BulkExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)
    async for df in res:
        expected_cols = {col.name for col in schema_definition.columns}
        actual_cols = set(df.columns)
        assert expected_cols != actual_cols

@pytest.mark.asyncio
async def test_readexcel_wrong_types_bulk(schema_definition, metrics):
    comp = ReadExcel(
        name="ReadExcel_Bulk_WrongTypes",
        description="Wrong data types",
        comp_type="read_excel",
        filepath=DATA_DIR / "test_data_types.xlsx",
        schema=schema_definition,
    )
    comp.strategy = BulkExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)
    async for df in res:
        assert isinstance(df, pd.DataFrame)
        # all values should be read as object (string) instead of numeric
        assert df["id"].dtype == object

@pytest.mark.asyncio
async def test_readexcel_invalid_file(metrics, schema_definition, tmp_path: Path):
    # Create an invalid Excel file (actually just text)
    invalid_fp = tmp_path / "invalid.xlsx"
    invalid_fp.write_text("not a real excel file")

    comp = ReadExcel(
        name="ReadExcel_Invalid",
        description="Invalid Excel file",
        comp_type="read_excel",
        filepath=invalid_fp,
        schema=schema_definition,
    )
    comp.strategy = BulkExecutionStrategy()

    with pytest.raises(Exception):
        res = comp.execute(payload=None, metrics=metrics)
        async for _ in res:
            pass