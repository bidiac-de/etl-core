from __future__ import annotations

from pathlib import Path

import pytest

# Ensure components are registered
import etl_core.components.file_components.csv.read_csv  # noqa: F401
import etl_core.components.file_components.csv.write_csv  # noqa: F401
import etl_core.components.file_components.json.read_json  # noqa: F401
import etl_core.components.file_components.json.write_json  # noqa: F401
import etl_core.components.file_components.excel.read_excel  # noqa: F401
import etl_core.components.file_components.excel.write_excel  # noqa: F401
import etl_core.components.data_operations.aggregation.aggregation_component  # noqa: F401, E501

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


def test_csv_row_aggregation_count_by_name_to_csv(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("csv_row_agg_count_by_name_csv.json")
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

    out = read_output("write_csv", cfg["strategy_type"], out_fp).sort_values("name")
    # Expect one row per name with count == 1
    assert list(out["name"]) == ["Alice", "Bob", "Charlie"]
    assert list(out["n"]) == [1, 1, 1]


def test_csv_bulk_aggregation_count_to_excel(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("csv_bulk_agg_count_excel.json")
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

    out = read_output("write_excel", cfg["strategy_type"], out_fp)
    assert out.shape[0] == 1
    assert int(out.loc[0, "n"]) == 3  # total rows


def test_excel_bulk_aggregation_nunique_names_to_json(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("excel_bulk_agg_nunique_json.json")
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

    out = read_output("write_json", cfg["strategy_type"], out_fp)
    # Distinct names: Alice, Bob, Charlie
    assert out.shape[0] == 1
    assert int(out.loc[0, "names"]) == 3


def test_json_row_aggregation_id_stats_to_csv(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("json_row_agg_id_stats_csv.json")
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

    out = read_output("write_csv", cfg["strategy_type"], out_fp)
    assert out.shape[0] == 1
    row = out.iloc[0]
    assert int(row["id_min"]) == 1
    assert int(row["id_max"]) == 3
    assert int(row["id_sum"]) == 6
    assert pytest.approx(float(row["id_mean"]), rel=1e-6) == 2
    assert pytest.approx(float(row["id_median"]), rel=1e-6) == 2
    assert pytest.approx(float(row["id_std"]), rel=1e-6) == 1
    assert int(row["n"]) == 3


def test_json_bigdata_aggregation_by_name_to_json_dir(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("json_bigdata_agg_count_by_name_json.json")
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

    out = read_output("write_json", cfg["strategy_type"], out_dir).sort_values("name")
    assert list(out["name"]) == ["Alice", "Bob", "Charlie"]
    assert list(out["n"]) == [1, 1, 1]


def test_json_bulk_aggregation_minmax_name_to_json(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("json_bulk_agg_minmax_name_json.json")
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

    out = read_output("write_json", cfg["strategy_type"], out_fp)
    assert out.shape[0] == 1
    row = out.iloc[0]
    assert str(row["name_min"]) == "Alice"
    assert str(row["name_max"]) == "Charlie"
