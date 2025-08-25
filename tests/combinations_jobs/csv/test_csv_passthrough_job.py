from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import dask.dataframe as dd
import pandas as pd
import pytest

import etl_core.components.file_components.csv.read_csv  # noqa: F401
import etl_core.components.file_components.csv.write_csv  # noqa: F401

from etl_core.components.runtime_state import RuntimeState
from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from tests.helpers import runtime_job_from_config


ROW_JOB = Path(__file__).parent / "csv_passthrough_row.json"
BULK_JOB = Path(__file__).parent / "csv_passthrough_bulk.json"
BIG_JOB = Path(__file__).parent / "csv_passthrough_bigdata.json"


def _load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _parse_json(text: str) -> Dict[str, Any]:
    return json.loads(text)


def _normalize_components(components: List[Dict[str, Any]]) -> None:
    """
    Normalize components: ensure comp_type, description, options
    """
    for comp in components:
        ctype = comp.get("comp_type") or comp.get("type")
        if not ctype:
            raise AssertionError("Component missing 'comp_type'/'type'")
        comp["comp_type"] = ctype
        if "type" in comp:
            del comp["type"]

        if "description" not in comp or comp["description"] is None:
            comp["description"] = ""

        if "options" not in comp or comp["options"] is None:
            comp["options"] = {}


def _inject_filepaths(
    components: List[Dict[str, Any]], in_fp: Path, out_fp: Path
) -> None:
    """
    Inject temp input/output file paths.
    """
    for comp in components:
        ctype = comp.get("comp_type")
        if ctype == "read_csv":
            comp["filepath"] = str(in_fp)
        elif ctype == "write_csv":
            comp["filepath"] = str(out_fp)


def _render_job_cfg(cfg_path: Path, in_fp: Path, out_fp: Path) -> Dict[str, Any]:
    """
    Load JSON, normalize schema, inject paths.
    """
    text = _load_text(cfg_path)
    if "{{IN}}" in text or "{{OUT}}" in text:
        text = text.replace("{{IN}}", str(in_fp)).replace("{{OUT}}", str(out_fp))
    cfg = _parse_json(text)

    comps = cfg.get("components", [])
    _normalize_components(comps)

    _inject_filepaths(comps, in_fp, out_fp)
    return cfg


@pytest.fixture()
def exec_handler() -> JobExecutionHandler:
    """Return JobExecutionHandler fixture."""
    return JobExecutionHandler()


def test_execute_csv_row_job(tmp_path: Path, exec_handler: JobExecutionHandler) -> None:
    """Row strategy: read/write a single line CSV."""
    in_fp = tmp_path / "in.csv"
    pd.DataFrame([{"id": 1, "name": "Nina"}]).to_csv(in_fp, index=False)
    out_fp = tmp_path / "out.csv"

    cfg = _render_job_cfg(ROW_JOB, in_fp, out_fp)
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

    cfg = _render_job_cfg(BULK_JOB, in_fp, out_fp)
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

    cfg = _render_job_cfg(BIG_JOB, in_fp, out_fp)
    job = runtime_job_from_config(cfg)

    execution = exec_handler.execute_job(job)
    mh = exec_handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    ddf = dd.read_csv(str(out_fp), assume_missing=True, dtype=str)
    df_out = ddf.compute().sort_values("id")
    assert list(df_out["name"]) == ["Max", "Gina"]
