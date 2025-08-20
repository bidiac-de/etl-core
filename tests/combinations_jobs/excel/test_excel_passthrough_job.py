import etl_core.components.file_components.excel.read_excel  # noqa: F401
import etl_core.components.file_components.excel.write_excel  # noqa: F401

import json
from pathlib import Path

import pandas as pd

from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from etl_core.components.runtime_state import RuntimeState
from tests.helpers import runtime_job_from_config


def _load_cfg(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _make_job(cfg_path: Path, in_path: Path, out_path: Path):
    cfg = _load_cfg(cfg_path)
    for comp in cfg["components"]:
        if comp["comp_type"] == "read_excel":
            comp["filepath"] = str(in_path)
        if comp["comp_type"] == "write_excel":
            comp["filepath"] = str(out_path)
    return runtime_job_from_config(cfg)


def test_execute_excel_row_job(tmp_path: Path) -> None:
    # Row-Streaming: Excel (.xlsx)
    in_fp = tmp_path / "in.xlsx"
    df_in = pd.DataFrame([{"id": 1, "name": "Nina"}, {"id": 2, "name": "Max"}])
    df_in.to_excel(in_fp, index=False)

    out_fp = tmp_path / "out.xlsx"

    cfg_path = Path(__file__).parent / "job_config_excel_row.json"
    job = _make_job(cfg_path, in_fp, out_fp)

    handler = JobExecutionHandler()
    execution = handler.execute_job(job)

    assert (
        handler.job_info.metrics_handler.get_job_metrics(execution.id).status
        == RuntimeState.SUCCESS
    )
    out = pd.read_excel(out_fp)
    assert list(out["name"]) == ["Nina", "Max"]


def test_execute_excel_bulk_job(tmp_path: Path) -> None:
    # Bulk: Excel (.xlsx)
    in_fp = tmp_path / "in_bulk.xlsx"
    df_in = pd.DataFrame([{"id": 2, "name": "Omar"}, {"id": 3, "name": "Lina"}])
    df_in.to_excel(in_fp, index=False)

    out_fp = tmp_path / "out_bulk.xlsx"

    cfg_path = Path(__file__).parent / "job_config_excel_bulk.json"
    job = _make_job(cfg_path, in_fp, out_fp)

    handler = JobExecutionHandler()
    execution = handler.execute_job(job)

    assert (
        handler.job_info.metrics_handler.get_job_metrics(execution.id).status
        == RuntimeState.SUCCESS
    )
    out = pd.read_excel(out_fp).sort_values("id")
    assert list(out["name"]) == ["Omar", "Lina"]


def test_execute_excel_bigdata_job(tmp_path: Path) -> None:
    # BigData: Excel in/out (.xlsx) — read as Dask internally, written back to Excel
    in_fp = tmp_path / "in_big.xlsx"
    df_in = pd.DataFrame([{"id": 4, "name": "Max"}, {"id": 5, "name": "Gina"}])
    df_in.to_excel(in_fp, index=False)

    out_fp = tmp_path / "out_big.xlsx"

    cfg_path = Path(__file__).parent / "job_config_excel_bigdata.json"
    job = _make_job(cfg_path, in_fp, out_fp)

    handler = JobExecutionHandler()
    execution = handler.execute_job(job)

    assert (
        handler.job_info.metrics_handler.get_job_metrics(execution.id).status
        == RuntimeState.SUCCESS
    )
    out = pd.read_excel(out_fp).sort_values("id")
    assert list(out["name"]) == ["Max", "Gina"]
