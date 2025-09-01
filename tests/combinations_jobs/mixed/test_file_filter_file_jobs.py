from __future__ import annotations

from pathlib import Path

import dask.dataframe as dd
import pytest

# Ensure components are registered
import etl_core.components.file_components.csv.read_csv  # noqa: F401
import etl_core.components.file_components.csv.write_csv  # noqa: F401
import etl_core.components.file_components.json.read_json  # noqa: F401
import etl_core.components.file_components.json.write_json  # noqa: F401
import etl_core.components.file_components.excel.read_excel  # noqa: F401
import etl_core.components.file_components.excel.write_excel  # noqa: F401
import etl_core.components.data_operations.filter.filter_component  # noqa: F401

from etl_core.components.runtime_state import RuntimeState
from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from tests.helpers import runtime_job_from_config
from tests.config_helpers import (
    render_job_cfg_with_filepaths,
    read_output,
    data_path,
)


def _cfg_dir() -> Path:
    # JSON configs live next to this test file
    return Path(__file__).with_suffix("").parent


def _cfg(path_name: str) -> Path:
    return _cfg_dir() / path_name


@pytest.fixture()
def exec_handler() -> JobExecutionHandler:
    return JobExecutionHandler()


CSV_VALID = data_path("tests", "components", "data", "csv", "test_data.csv")
JSON_VALID_NDJSON = data_path("tests", "components", "data", "json", "testdata.jsonl")
JSON_VALID_ARRAY = data_path("tests", "components", "data", "json", "testdata.json")
EXCEL_VALID = data_path("tests", "components", "data", "excel", "test_data.xlsx")


def test_csv_row_filter_to_csv(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("csv_row_filter_csv.json")
    out_fp = tmp_path / "out_row.csv"

    cfg = render_job_cfg_with_filepaths(
        cfg_path, {"read_csv": CSV_VALID, "write_csv": out_fp}
    )
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.SUCCESS
    )

    out = read_output("write_csv", cfg["strategy_type"], out_fp).sort_values("id")
    assert list(out["name"]) == ["Alice", "Charlie"]


def test_csv_bigdata_filter_to_csv(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("csv_bigdata_filter_csv.json")
    out_fp = tmp_path / "out_big.csv"

    cfg = render_job_cfg_with_filepaths(
        cfg_path, {"read_csv": CSV_VALID, "write_csv": out_fp}
    )
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.SUCCESS
    )

    ddf = dd.read_csv(str(out_fp), assume_missing=True, dtype=str)
    out = ddf.compute().sort_values("id")
    assert list(out["name"]) == ["Alice", "Charlie"]


def test_json_row_filter_to_csv(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("json_row_filter_csv.json")
    out_fp = tmp_path / "out_row.csv"

    cfg = render_job_cfg_with_filepaths(
        cfg_path, {"read_json": JSON_VALID_NDJSON, "write_csv": out_fp}
    )
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.SUCCESS
    )

    out = read_output("write_csv", cfg["strategy_type"], out_fp).sort_values("id")
    assert list(out["name"]) == ["Alice", "Charlie"]


def test_json_bulk_filter_to_json(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("json_bulk_filter_json.json")
    out_fp = tmp_path / "out_bulk.json"

    cfg = render_job_cfg_with_filepaths(
        cfg_path, {"read_json": JSON_VALID_ARRAY, "write_json": out_fp}
    )
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.SUCCESS
    )

    out = read_output("write_json", cfg["strategy_type"], out_fp).sort_values("id")
    assert list(out["name"]) == ["Bob", "Charlie"]


def test_json_bigdata_filter_to_json_dir(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("json_bigdata_filter_json.json")
    out_dir = tmp_path / "out_big_ndjson"
    out_dir.mkdir(parents=True, exist_ok=True)

    cfg = render_job_cfg_with_filepaths(
        cfg_path, {"read_json": JSON_VALID_NDJSON, "write_json": out_dir}
    )
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.SUCCESS
    )

    out = read_output("write_json", cfg["strategy_type"], out_dir).sort_values("id")
    assert list(out["name"]) == ["Alice", "Charlie"]


def test_excel_bulk_filter_to_excel(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("excel_bulk_filter_excel.json")
    out_fp = tmp_path / "out_bulk.xlsx"

    cfg = render_job_cfg_with_filepaths(
        cfg_path, {"read_excel": EXCEL_VALID, "write_excel": out_fp}
    )
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.SUCCESS
    )

    out = read_output("write_excel", cfg["strategy_type"], out_fp).sort_values("id")
    assert list(out["name"]) == ["Alice", "Charlie"]


def test_excel_bulk_filter_to_csv(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("excel_bulk_filter_csv.json")
    out_fp = tmp_path / "out_bulk.csv"

    cfg = render_job_cfg_with_filepaths(
        cfg_path, {"read_excel": EXCEL_VALID, "write_csv": out_fp}
    )
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.SUCCESS
    )

    out = read_output("write_csv", cfg["strategy_type"], out_fp).sort_values("id")
    assert list(out["name"]) == ["Alice", "Charlie"]


def test_csv_bulk_filter_to_excel(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("csv_bulk_filter_excel.json")
    out_fp = tmp_path / "out_bulk.xlsx"

    cfg = render_job_cfg_with_filepaths(
        cfg_path, {"read_csv": CSV_VALID, "write_excel": out_fp}
    )
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.SUCCESS
    )

    out = read_output("write_excel", cfg["strategy_type"], out_fp).sort_values("id")
    assert list(out["name"]) == ["Alice", "Charlie"]


def test_excel_bulk_filter_to_json(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("excel_bulk_filter_json.json")
    out_fp = tmp_path / "out_bulk.json"

    cfg = render_job_cfg_with_filepaths(
        cfg_path, {"read_excel": EXCEL_VALID, "write_json": out_fp}
    )
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.SUCCESS
    )

    out = read_output("write_json", cfg["strategy_type"], out_fp).sort_values("id")
    assert list(out["name"]) == ["Alice", "Charlie"]
