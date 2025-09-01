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

from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.files.excel.excel_receiver import ExcelReceiver
from tests.helpers import normalize_df


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
def sample_excel_file() -> Path:
    return (
        Path(__file__).parent.parent
        / "components"
        / "data"
        / "excel"
        / "test_data.xlsx"
    )


@pytest.fixture(scope="session")
def expected_df(sample_excel_file: Path) -> pd.DataFrame:
    df = pd.read_excel(sample_excel_file)
    return df[[*df.columns]]


@pytest.mark.asyncio
async def test_excelreceiver_read_row_streaming_wholeframe(
    sample_excel_file: Path,
    expected_df: pd.DataFrame,
    metrics: ComponentMetrics,
) -> None:
    """
    Row mode: consume via async generator with a per-row timeout,
    then compare the WHOLE output as a DataFrame to the expected_df.
    """
    receiver = ExcelReceiver()
    rows = receiver.read_row(filepath=sample_excel_file, metrics=metrics)

    assert inspect.isasyncgen(rows) or isinstance(rows, AsyncGenerator)

    collected: List[Dict[str, Any]] = []
    try:
        while True:
            item = await asyncio.wait_for(anext(rows), timeout=0.5)
            collected.append(item)
    except StopAsyncIteration:
        pass
    finally:
        await rows.aclose()

    actual_df = pd.DataFrame(collected)
    lhs = normalize_df(actual_df)
    rhs = normalize_df(expected_df)

    assert_frame_equal(lhs, rhs, check_dtype=False, check_exact=False)
    assert metrics.error_count == 0
    assert metrics.lines_received == len(expected_df)


@pytest.mark.asyncio
async def test_excelreceiver_read_bulk_matches_expected(
    sample_excel_file: Path,
    expected_df: pd.DataFrame,
    metrics: ComponentMetrics,
) -> None:
    receiver = ExcelReceiver()
    df = await receiver.read_bulk(filepath=sample_excel_file, metrics=metrics)
    lhs = normalize_df(df)
    rhs = normalize_df(expected_df)
    assert_frame_equal(lhs, rhs, check_dtype=False, check_exact=False)
    assert metrics.error_count == 0
    assert metrics.lines_received == len(expected_df)


@pytest.mark.asyncio
async def test_excelreceiver_read_bigdata_matches_expected(
    sample_excel_file: Path,
    expected_df: pd.DataFrame,
    metrics: ComponentMetrics,
) -> None:
    receiver = ExcelReceiver()
    ddf = await receiver.read_bigdata(filepath=sample_excel_file, metrics=metrics)
    assert isinstance(ddf, dd.DataFrame)

    actual = normalize_df(ddf.compute())
    expected = normalize_df(expected_df)
    assert_eq(actual, expected, check_dtype=False, check_index=False)
    assert metrics.error_count == 0
    assert metrics.lines_received == len(expected_df)


@pytest.mark.asyncio
async def test_excelreceiver_write_row_from_expected_df(
    tmp_path: Path,
    expected_df: pd.DataFrame,
    metrics: ComponentMetrics,
) -> None:
    """
    Write-row behaves like read-row: each call is awaited with a timeout and
    we assert incrementally after every write. Final check compares the whole
    written file against the full expected_df.
    """
    out_fp = tmp_path / "out_row.xlsx"
    receiver = ExcelReceiver()

    for i, rec in enumerate(expected_df.to_dict(orient="records"), start=1):
        await asyncio.wait_for(
            receiver.write_row(filepath=out_fp, metrics=metrics, row=rec),
            timeout=0.5,
        )
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
async def test_excelreceiver_write_bulk_from_expected_df(
    tmp_path: Path,
    expected_df: pd.DataFrame,
    metrics: ComponentMetrics,
) -> None:
    out_fp = tmp_path / "out_bulk.xlsx"
    receiver = ExcelReceiver()

    await receiver.write_bulk(filepath=out_fp, metrics=metrics, data=expected_df)
    assert out_fp.exists()

    actual = pd.read_excel(out_fp)
    lhs = normalize_df(actual)
    rhs = normalize_df(expected_df)
    assert_frame_equal(lhs, rhs, check_dtype=False, check_exact=False)

    assert metrics.error_count == 0
    assert metrics.lines_forwarded == len(expected_df)


@pytest.mark.asyncio
async def test_excelreceiver_write_bigdata_from_expected_df(
    tmp_path: Path,
    expected_df: pd.DataFrame,
    metrics: ComponentMetrics,
) -> None:
    out_fp = tmp_path / "out_big.xlsx"
    receiver = ExcelReceiver()

    ddf_in = dd.from_pandas(expected_df, npartitions=1)
    await receiver.write_bigdata(filepath=out_fp, metrics=metrics, data=ddf_in)

    assert out_fp.exists()
    actual = pd.read_excel(out_fp)

    lhs = normalize_df(actual)
    rhs = normalize_df(expected_df)
    assert_frame_equal(lhs, rhs, check_dtype=False, check_exact=False)

    assert metrics.error_count == 0
    assert metrics.lines_forwarded == len(expected_df)


@pytest.mark.asyncio
async def test_excelreceiver_missing_file_bulk_raises(
    metrics: ComponentMetrics, tmp_path: Path
) -> None:
    receiver = ExcelReceiver()
    with pytest.raises(FileNotFoundError):
        await receiver.read_bulk(filepath=tmp_path / "missing.xlsx", metrics=metrics)


@pytest.mark.asyncio
async def test_excelreceiver_invalid_file_raises(
    metrics: ComponentMetrics, tmp_path: Path
) -> None:
    invalid = tmp_path / "invalid.xlsx"
    invalid.write_text("not an excel file")
    receiver = ExcelReceiver()
    with pytest.raises(Exception):
        await receiver.read_bulk(filepath=invalid, metrics=metrics)
