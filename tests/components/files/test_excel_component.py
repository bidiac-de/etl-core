import asyncio
import inspect
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List

import dask.dataframe as dd
from dask.dataframe.utils import assert_eq
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from etl_core.components.envelopes import Out
from etl_core.components.file_components.excel.read_excel import ReadExcel
from etl_core.components.file_components.excel.write_excel import WriteExcel
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.strategies.bigdata_strategy import BigDataExecutionStrategy
from etl_core.strategies.bulk_strategy import BulkExecutionStrategy
from etl_core.strategies.row_strategy import RowExecutionStrategy
from tests.helpers import normalize_df


DATA_DIR = Path(__file__).parent.parent / "data" / "excel"
VALID_XLSX = DATA_DIR / "test_data.xlsx"
MISSING_VALUES_XLSX = DATA_DIR / "test_data_missing.xlsx"
SCHEMA_MISMATCH_XLSX = DATA_DIR / "test_data_schema.xlsx"
WRONG_TYPES_XLSX = DATA_DIR / "test_data_types.xlsx"


@pytest.fixture
def metrics() -> ComponentMetrics:
    return ComponentMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
    )


@pytest.fixture(scope="session")
def expected_df() -> pd.DataFrame:
    df = pd.read_excel(VALID_XLSX)
    return df[[*df.columns]]


@pytest.fixture(scope="session")
def expected_missing_df() -> pd.DataFrame:
    df = pd.read_excel(MISSING_VALUES_XLSX)
    return df[[*df.columns]]


@pytest.mark.asyncio
async def test_readexcel_valid_bulk_wholeframe(
    metrics: ComponentMetrics, expected_df: pd.DataFrame
) -> None:
    comp = ReadExcel(
        name="ReadExcel_Bulk_Valid",
        description="Valid Excel file",
        comp_type="read_excel",
        filepath=VALID_XLSX,
    )
    comp.strategy = BulkExecutionStrategy()

    gen = comp.execute(payload=None, metrics=metrics)
    assert inspect.isasyncgen(gen) or isinstance(gen, AsyncGenerator)

    items: List[Out] = []
    async for item in gen:
        items.append(item)

    assert len(items) == 1
    assert items[0].port == "out"
    lhs = normalize_df(items[0].payload)
    rhs = normalize_df(expected_df)
    assert_frame_equal(lhs, rhs, check_dtype=False, check_exact=False)
    assert metrics.error_count == 0
    assert metrics.lines_received == len(expected_df)


@pytest.mark.asyncio
async def test_readexcel_row_streaming_wholeframe(
    metrics: ComponentMetrics, expected_df: pd.DataFrame
) -> None:
    comp = ReadExcel(
        name="ReadExcel_Row_Stream",
        description="Row streaming",
        comp_type="read_excel",
        filepath=VALID_XLSX,
    )
    comp.strategy = RowExecutionStrategy()

    rows = comp.execute(payload=None, metrics=metrics)
    assert inspect.isasyncgen(rows) or isinstance(rows, AsyncGenerator)

    collected: List[Dict[str, Any]] = []
    try:
        while True:
            item = await asyncio.wait_for(anext(rows), timeout=0.5)
            assert isinstance(item, Out) and item.port == "out"
            collected.append(item.payload)
    except StopAsyncIteration:
        pass
    finally:
        await rows.aclose()

    actual = normalize_df(pd.DataFrame(collected))
    expected = normalize_df(expected_df)
    assert_frame_equal(actual, expected, check_dtype=False, check_exact=False)
    assert metrics.error_count == 0
    assert metrics.lines_received == len(expected_df)


@pytest.mark.asyncio
async def test_readexcel_bigdata_wholeframe(
    metrics: ComponentMetrics, expected_df: pd.DataFrame
) -> None:
    comp = ReadExcel(
        name="ReadExcel_BigData",
        description="Read Excel with Dask",
        comp_type="read_excel",
        filepath=VALID_XLSX,
    )
    comp.strategy = BigDataExecutionStrategy()

    gen = comp.execute(payload=None, metrics=metrics)
    items: List[Out] = []
    async for item in gen:
        items.append(item)

    assert len(items) == 1
    assert items[0].port == "out"
    actual_pdf = normalize_df(items[0].payload.compute())
    expected_pdf = normalize_df(expected_df)
    assert_eq(actual_pdf, expected_pdf, check_dtype=False, check_index=False)
    assert metrics.error_count == 0
    assert metrics.lines_received == len(expected_df)


@pytest.mark.asyncio
async def test_readexcel_missing_values_bulk_wholeframe(
    metrics: ComponentMetrics, expected_missing_df: pd.DataFrame
) -> None:
    comp = ReadExcel(
        name="ReadExcel_Bulk_Missing",
        description="Missing values",
        comp_type="read_excel",
        filepath=MISSING_VALUES_XLSX,
    )
    comp.strategy = BulkExecutionStrategy()

    gen = comp.execute(payload=None, metrics=metrics)
    items: List[Out] = []
    async for item in gen:
        items.append(item)

    assert len(items) == 1
    assert items[0].port == "out"
    lhs = normalize_df(items[0].payload)
    rhs = normalize_df(expected_missing_df)
    assert_frame_equal(lhs, rhs, check_dtype=False, check_exact=False)
    assert lhs.isna().any().any()
    assert metrics.error_count == 0


@pytest.mark.asyncio
async def test_readexcel_wrong_types_bulk_does_not_crash(
    metrics: ComponentMetrics,
) -> None:
    comp = ReadExcel(
        name="ReadExcel_Bulk_WrongTypes",
        description="Wrong types",
        comp_type="read_excel",
        filepath=WRONG_TYPES_XLSX,
    )
    comp.strategy = BulkExecutionStrategy()

    gen = comp.execute(payload=None, metrics=metrics)
    items: List[Out] = []
    async for item in gen:
        items.append(item)

    assert len(items) == 1
    assert items[0].port == "out"
    df = items[0].payload
    assert {"id", "name"}.issubset(df.columns)
    df["id"].astype(str)
    assert metrics.error_count == 0


@pytest.mark.asyncio
async def test_writeexcel_row_with_timeouts_and_incremental_asserts(
    tmp_path: Path, metrics: ComponentMetrics, expected_df: pd.DataFrame
) -> None:
    out_fp = tmp_path / "row_out.xlsx"
    comp = WriteExcel(
        name="WriteExcel_Row",
        description="Write single rows with incremental checks",
        comp_type="write_excel",
        filepath=out_fp,
    )
    comp.strategy = RowExecutionStrategy()

    for i, rec in enumerate(expected_df.to_dict(orient="records"), start=1):
        item = await asyncio.wait_for(
            anext(comp.execute(payload=rec, metrics=metrics), None),
            timeout=0.5,
        )
        assert isinstance(item, Out) and item.port == "out"
        assert item.payload == rec

        assert out_fp.exists()
        actual_partial = pd.read_excel(out_fp)
        lhs = normalize_df(actual_partial)
        rhs = normalize_df(expected_df.iloc[:i, :])
        assert_frame_equal(lhs, rhs, check_dtype=False, check_exact=False)
        assert metrics.error_count == 0
        assert metrics.lines_forwarded == i

    final_actual = pd.read_excel(out_fp)
    lhs = normalize_df(final_actual)
    rhs = normalize_df(expected_df)
    assert_frame_equal(lhs, rhs, check_dtype=False, check_exact=False)


@pytest.mark.asyncio
async def test_writeexcel_bulk_wholeframe(
    tmp_path: Path, metrics: ComponentMetrics, expected_df: pd.DataFrame
) -> None:
    out_fp = tmp_path / "bulk_out.xlsx"
    comp = WriteExcel(
        name="WriteExcel_Bulk",
        description="Write DataFrame",
        comp_type="write_excel",
        filepath=out_fp,
    )
    comp.strategy = BulkExecutionStrategy()

    item = await anext(comp.execute(payload=expected_df, metrics=metrics), None)
    assert isinstance(item, Out) and item.port == "out"
    pd.testing.assert_frame_equal(
        normalize_df(item.payload),
        normalize_df(expected_df),
        check_dtype=False,
        check_exact=False,
    )

    assert out_fp.exists()
    actual = pd.read_excel(out_fp)
    lhs = normalize_df(actual)
    rhs = normalize_df(expected_df)
    assert_frame_equal(lhs, rhs, check_dtype=False, check_exact=False)
    assert metrics.error_count == 0
    assert metrics.lines_forwarded == len(expected_df)


@pytest.mark.asyncio
async def test_writeexcel_bigdata_wholeframe(
    tmp_path: Path, metrics: ComponentMetrics, expected_df: pd.DataFrame
) -> None:
    out_fp = tmp_path / "big_out.xlsx"
    comp = WriteExcel(
        name="WriteExcel_BigData",
        description="Write Dask DataFrame",
        comp_type="write_excel",
        filepath=out_fp,
    )
    comp.strategy = BigDataExecutionStrategy()

    ddf_in = dd.from_pandas(expected_df, npartitions=1)
    item = await anext(comp.execute(payload=ddf_in, metrics=metrics), None)
    assert isinstance(item, Out) and item.port == "out"
    assert_eq(
        normalize_df(item.payload.compute()),
        normalize_df(expected_df),
        check_dtype=False,
        check_index=False,
    )

    assert out_fp.exists()
    actual = pd.read_excel(out_fp)
    lhs = normalize_df(actual)
    rhs = normalize_df(expected_df)
    assert_frame_equal(lhs, rhs, check_dtype=False, check_exact=False)
    assert metrics.error_count == 0
    assert metrics.lines_forwarded == len(expected_df)


@pytest.mark.asyncio
async def test_readexcel_invalid_file(
    metrics: ComponentMetrics, tmp_path: Path
) -> None:
    invalid = tmp_path / "invalid.xlsx"
    invalid.write_text("not a real excel file")
    comp = ReadExcel(
        name="ReadExcel_Invalid",
        description="Invalid Excel file",
        comp_type="read_excel",
        filepath=invalid,
    )
    comp.strategy = BulkExecutionStrategy()

    gen = comp.execute(payload=None, metrics=metrics)
    with pytest.raises(Exception):
        await anext(gen)


@pytest.mark.asyncio
async def test_readexcel_missing_file(
    metrics: ComponentMetrics, tmp_path: Path
) -> None:
    missing = tmp_path / "missing.xlsx"
    comp = ReadExcel(
        name="ReadExcel_Missing",
        description="Missing file",
        comp_type="read_excel",
        filepath=missing,
    )
    comp.strategy = BulkExecutionStrategy()

    gen = comp.execute(payload=None, metrics=metrics)
    with pytest.raises(FileNotFoundError):
        await anext(gen)
