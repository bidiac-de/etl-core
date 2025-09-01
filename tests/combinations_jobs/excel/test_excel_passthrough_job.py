import etl_core.components.file_components.excel.read_excel  # noqa: F401
import etl_core.components.file_components.excel.write_excel  # noqa: F401

from pathlib import Path
import pandas as pd

from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from etl_core.components.runtime_state import RuntimeState
from tests.helpers import runtime_job_from_config
from tests.config_helpers import render_job_cfg_with_filepaths


def test_execute_excel_row_job(tmp_path: Path) -> None:
    in_fp = tmp_path / "in.xlsx"
    df_in = pd.DataFrame([{"id": 1, "name": "Nina"}, {"id": 2, "name": "Max"}])
    df_in.to_excel(in_fp, index=False)

    out_fp = tmp_path / "out.xlsx"

    cfg_path = Path(__file__).parent / "job_config_excel_row.json"
    cfg = render_job_cfg_with_filepaths(
        cfg_path,
        {"read_excel": in_fp, "write_excel": out_fp},
    )
    job = runtime_job_from_config(cfg)

    handler = JobExecutionHandler()
    execution = handler.execute_job(job)

    assert (
        handler.job_info.metrics_handler.get_job_metrics(execution.id).status
        == RuntimeState.SUCCESS
    )
    out = pd.read_excel(out_fp)
    assert list(out["name"]) == ["Nina", "Max"]


def test_execute_excel_bulk_job(tmp_path: Path) -> None:
    in_fp = tmp_path / "in_bulk.xlsx"
    df_in = pd.DataFrame([{"id": 2, "name": "Omar"}, {"id": 3, "name": "Lina"}])
    df_in.to_excel(in_fp, index=False)

    out_fp = tmp_path / "out_bulk.xlsx"

    cfg_path = Path(__file__).parent / "job_config_excel_bulk.json"
    cfg = render_job_cfg_with_filepaths(
        cfg_path,
        {"read_excel": in_fp, "write_excel": out_fp},
    )
    job = runtime_job_from_config(cfg)

    handler = JobExecutionHandler()
    execution = handler.execute_job(job)

    assert (
        handler.job_info.metrics_handler.get_job_metrics(execution.id).status
        == RuntimeState.SUCCESS
    )
    out = pd.read_excel(out_fp).sort_values("id")
    assert list(out["name"]) == ["Omar", "Lina"]


def test_execute_excel_bigdata_job(tmp_path: Path) -> None:
    in_fp = tmp_path / "in_big.xlsx"
    df_in = pd.DataFrame([{"id": 4, "name": "Max"}, {"id": 5, "name": "Gina"}])
    df_in.to_excel(in_fp, index=False)

    out_fp = tmp_path / "out_big.xlsx"

    cfg_path = Path(__file__).parent / "job_config_excel_bigdata.json"
    cfg = render_job_cfg_with_filepaths(
        cfg_path,
        {"read_excel": in_fp, "write_excel": out_fp},
    )
    job = runtime_job_from_config(cfg)

    handler = JobExecutionHandler()
    execution = handler.execute_job(job)

    assert (
        handler.job_info.metrics_handler.get_job_metrics(execution.id).status
        == RuntimeState.SUCCESS
    )
    out = pd.read_excel(out_fp).sort_values("id")
    assert list(out["name"]) == ["Max", "Gina"]
