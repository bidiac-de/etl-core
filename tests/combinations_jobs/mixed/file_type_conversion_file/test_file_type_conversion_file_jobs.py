from __future__ import annotations

from pathlib import Path

import dask.dataframe as dd
import pytest

import etl_core.components.file_components.csv.read_csv  # noqa: F401
import etl_core.components.file_components.csv.write_csv  # noqa: F401
import etl_core.components.file_components.json.read_json  # noqa: F401
import etl_core.components.file_components.json.write_json  # noqa: F401
import etl_core.components.file_components.excel.read_excel  # noqa: F401
import etl_core.components.file_components.excel.write_excel  # noqa: F401
import etl_core.components.data_operations.type_conversion.type_conversion_component  # noqa: F401, E501

from etl_core.components.runtime_state import RuntimeState
from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from tests.helpers import runtime_job_from_config
from tests.config_helpers import (
    render_job_cfg_with_filepaths,
    read_output,
    data_path,
)


def _cfg_dir() -> Path:
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


def test_json_row_typeconv_to_csv__ok(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:

    cfg_path = _cfg("json_row_typeconv_csv.json")
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
    assert str(out.dtypes["id"]).startswith(("int", "Int"))
    assert list(out["name"]) == ["Alice", "Bob", "Charlie"]


def test_json_row_typeconv_to_csv__onerror_null(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("json_row_typeconv_null_csv.json")
    out_fp = tmp_path / "out_row_null.csv"

    cfg = render_job_cfg_with_filepaths(
        cfg_path, {"read_json": JSON_VALID_NDJSON, "write_csv": out_fp}
    )
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.SUCCESS
    )

    out = read_output("write_csv", cfg["strategy_type"], out_fp)
    assert out["name"].isna().all()


def test_json_bulk_typeconv_to_json__onerror_raise_fails_job(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:

    cfg_path = _cfg("json_bulk_typeconv_json.json")
    out_fp = tmp_path / "out_bulk.json"

    cfg = render_job_cfg_with_filepaths(
        cfg_path, {"read_json": JSON_VALID_ARRAY, "write_json": out_fp}
    )
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.FAILED
    )


def test_json_bulk_typeconv_to_json__onerror_skip_keeps_originals(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:

    cfg_path = _cfg("json_bulk_typeconv_skip_json.json")
    out_fp = tmp_path / "out_bulk_skip.json"

    cfg = render_job_cfg_with_filepaths(
        cfg_path, {"read_json": JSON_VALID_ARRAY, "write_json": out_fp}
    )
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.SUCCESS
    )

    out = read_output("write_json", cfg["strategy_type"], out_fp)
    # SKIP: name bleibt String (keine Zahl in den Testdaten)
    assert out["name"].map(lambda v: isinstance(v, str)).all()


def test_csv_bigdata_typeconv_to_csv__dask_roundtrip(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:

    cfg_path = _cfg("csv_bigdata_typeconv_csv.json")
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
    assert list(out["name"]) == ["Alice", "Bob", "Charlie"]
