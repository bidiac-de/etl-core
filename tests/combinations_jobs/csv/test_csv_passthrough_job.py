from __future__ import annotations

from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

import etl_core.components.file_components.csv.read_csv  # noqa: F401
import etl_core.components.file_components.csv.write_csv  # noqa: F401

from etl_core.components.runtime_state import RuntimeState
from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from tests.helpers import runtime_job_from_config
from tests.config_helpers import render_job_cfg_with_filepaths


ROW_JOB = Path(__file__).parent / "csv_passthrough_row.json"
BULK_JOB = Path(__file__).parent / "csv_passthrough_bulk.json"
BIG_JOB = Path(__file__).parent / "csv_passthrough_bigdata.json"


@pytest.fixture()
def exec_handler() -> JobExecutionHandler:
    """Return JobExecutionHandler fixture."""
    return JobExecutionHandler()


def test_execute_csv_row_job(tmp_path: Path, exec_handler: JobExecutionHandler) -> None:
    """Row strategy: read/write a single line CSV."""
    in_fp = tmp_path / "in.csv"
    pd.DataFrame([{"id": 1, "name": "Nina"}]).to_csv(in_fp, index=False)
    out_fp = tmp_path / "out.csv"

    cfg = render_job_cfg_with_filepaths(
        ROW_JOB,
        {"read_csv": in_fp, "write_csv": out_fp},
    )
    job = runtime_job_from_config(cfg)

    execution = exec_handler.execute_job(job)
    mh = exec_handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS
    out = pd.read_csv(out_fp)
    assert list(out["name"]) == ["Nina"]


def test_execute_csv_bulk_job(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    """Bulk strategy: read/write two rows."""
    in_fp = tmp_path / "in_bulk.csv"
    pd.DataFrame([{"id": 2, "name": "Omar"}, {"id": 3, "name": "Lina"}]).to_csv(
        in_fp, index=False
    )
    out_fp = tmp_path / "out_bulk.csv"

    cfg = render_job_cfg_with_filepaths(
        BULK_JOB,
        {"read_csv": in_fp, "write_csv": out_fp},
    )
    job = runtime_job_from_config(cfg)

    execution = exec_handler.execute_job(job)
    mh = exec_handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS
    out = pd.read_csv(out_fp).sort_values("id")
    assert list(out["name"]) == ["Omar", "Lina"]


def test_execute_csv_bigdata_job(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    """BigData strategy: single-file CSV via Dask."""
    in_fp = tmp_path / "in_big.csv"
    pd.DataFrame([{"id": 4, "name": "Max"}, {"id": 5, "name": "Gina"}]).to_csv(
        in_fp, index=False
    )
    out_fp = tmp_path / "out_big.csv"

    cfg = render_job_cfg_with_filepaths(
        BIG_JOB,
        {"read_csv": in_fp, "write_csv": out_fp},
    )
    job = runtime_job_from_config(cfg)

    execution = exec_handler.execute_job(job)
    mh = exec_handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    ddf = dd.read_csv(str(out_fp), assume_missing=True, dtype=str)
    df_out = ddf.compute().sort_values("id")
    assert list(df_out["name"]) == ["Max", "Gina"]
