import asyncio
import inspect
import pytest
import pandas as pd
import dask.dataframe as dd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, AsyncGenerator

from etl_core.components.file_components.csv.read_csv import ReadCSV
from etl_core.components.file_components.csv.write_csv import WriteCSV
from etl_core.components.file_components.csv.csv_component import Delimiter
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.strategies.row_strategy import RowExecutionStrategy
from etl_core.strategies.bulk_strategy import BulkExecutionStrategy
from etl_core.strategies.bigdata_strategy import BigDataExecutionStrategy

DATA_DIR = Path(__file__).parent / "data/csv"
VALID_CSV = DATA_DIR / "test_data.csv"
MISSING_VALUES_CSV = DATA_DIR / "test_data_missing_values.csv"
WRONG_TYPES_CSV = DATA_DIR / "test_data_wrong_types.csv"
INVALID_CSV_FILE = DATA_DIR / "test_invalid_csv.csv"


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
async def test_read_csv_valid_bulk(metrics):
    comp = ReadCSV(
        name="ReadCSV_Bulk_Valid",
        description="Valid CSV file",
        comp_type="read_csv",
        filepath=VALID_CSV,
        separator=Delimiter.COMMA,
    )
    comp.strategy = BulkExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)
    assert isinstance(res, AsyncGenerator)

    async for item in res:
        assert isinstance(item, pd.DataFrame)
        assert len(item) == 3
        assert set(item.columns) == {"id", "name"}


@pytest.mark.asyncio
async def test_read_csv_missing_values_bulk(metrics):
    comp = ReadCSV(
        name="ReadCSV_Bulk_Missing",
        description="Missing values",
        comp_type="read_csv",
        filepath=MISSING_VALUES_CSV,
        separator=Delimiter.COMMA,
    )
    comp.strategy = BulkExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)
    async for item in res:
        assert isinstance(item, pd.DataFrame)
        assert len(item) == 3
        assert item.iloc[0, 0] == "1"
        assert item.iloc[0, 1] == "Alice"
        assert item.iloc[1, 0] == "2"
        assert pd.isna(item.iloc[1, 1])


@pytest.mark.asyncio
async def test_read_csv_row_streaming(metrics):
    comp = ReadCSV(
        name="ReadCSV_Row_Stream",
        description="Row streaming",
        comp_type="read_csv",
        filepath=VALID_CSV,
        separator=Delimiter.COMMA,
    )
    comp.strategy = RowExecutionStrategy()

    rows: Dict[str, Any] = comp.execute(payload=None, metrics=metrics)

    assert inspect.isasyncgen(rows) or isinstance(rows, AsyncGenerator)

    first = await asyncio.wait_for(anext(rows), timeout=0.25)
    assert set(first.keys()) == {"id", "name"}
    assert first["id"] == "1"
    assert first["name"] == "Alice"

    second = await asyncio.wait_for(anext(rows), timeout=0.25)
    assert set(second.keys()) == {"id", "name"}
    assert second["id"] == "2"
    assert second["name"] == "Bob"

    await rows.aclose()


@pytest.mark.asyncio
async def test_readcsv_bigdata(metrics):
    comp = ReadCSV(
        name="ReadCSV_BigData",
        description="Read CSV with Dask",
        comp_type="read_csv",
        filepath=VALID_CSV,
        separator=Delimiter.COMMA,
    )
    comp.strategy = BigDataExecutionStrategy()

    res = comp.execute(payload=None, metrics=metrics)

    async for item in res:
        assert isinstance(item, dd.DataFrame)
        pdf = item.compute()
        assert len(pdf) == 3
        assert pdf.iloc[0, 0] == "1"
        assert pdf.iloc[0, 1] == "Alice"
        assert pdf.iloc[1, 0] == "2"


@pytest.mark.asyncio
async def test_writecsv_row(tmp_path: Path, metrics):
    out_fp = tmp_path / "single.csv"

    comp = WriteCSV(
        name="WriteCSV_Row",
        description="Write single row",
        comp_type="write_csv",
        filepath=out_fp,
        separator=Delimiter.COMMA,
    )
    comp.strategy = RowExecutionStrategy()

    row = {"id": "1", "name": "Zoe"}
    await anext(comp.execute(payload=row, metrics=metrics), None)

    assert out_fp.exists()

    content = out_fp.read_text().splitlines()

    assert content[0] == "id,name"
    assert content[1] == "1,Zoe"


@pytest.mark.asyncio
async def test_writecsv_bulk(tmp_path: Path, metrics):
    out_fp = tmp_path / "bulk.csv"

    comp = WriteCSV(
        name="WriteCSV_Bulk",
        description="Write multiple rows",
        comp_type="write_csv",
        filepath=out_fp,
        separator=Delimiter.COMMA,
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

    content = out_fp.read_text().splitlines()

    assert content[0] == "id,name"
    assert content[1] == "1,A"
    assert content[2] == "2,B"
    assert content[3] == "3,C"


@pytest.mark.asyncio
async def test_writecsv_bigdata(tmp_path: Path, metrics):
    out_fp = tmp_path / "big.csv"

    comp = WriteCSV(
        name="WriteCSV_BigData",
        description="Write Dask DataFrame",
        comp_type="write_csv",
        filepath=out_fp,
        separator=Delimiter.COMMA,
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

    content = out_fp.read_text().splitlines()

    assert content[0] == "id,name"
    assert content[1] == "10,Nina"
    assert content[2] == "11,Omar"
