from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import dask.dataframe as dd
import pandas as pd
import pytest

from etl_core.components.runtime_state import RuntimeState
from etl_core.job_execution.job_execution_handler import JobExecutionHandler

from tests.helpers import runtime_job_from_config

CSV_JOB_TEMPLATE = Path(__file__).parent / "csv_passthrough_job.json"


def _load_job_cfg(path: Path) -> Dict[str, Any]:
    """Load a job configuration dictionary from a JSON file."""
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _patch_filepaths(
        cfg: Dict[str, Any],
        repl: List[Tuple[str, Path]],
) -> Dict[str, Any]:
    """
    Return a deep-copied config with component.filepath replaced.

    :param cfg: Original job config (dict)
    :param repl: List of (component_name, new_path)
    """
    cfg2 = json.loads(json.dumps(cfg))
    by_name = {c["name"]: c for c in cfg2.get("components", [])}
    for comp, p in repl:
        if comp not in by_name:
            raise KeyError(f"Component '{comp}' not found")
        by_name[comp]["filepath"] = str(p)
    return cfg2


def _with_strategy(cfg: Dict[str, Any], strategy: str) -> Dict[str, Any]:
    """Return a deep-copied config with .strategy_type overridden."""
    cfg2 = json.loads(json.dumps(cfg))
    cfg2["strategy_type"] = strategy
    return cfg2


def _normalize_job_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize historical / alternative config keys and datatypes.
    """
    norm = json.loads(json.dumps(cfg))

    if "name" not in norm and "job_name" in norm:
        norm["name"] = norm.pop("job_name")

    if isinstance(norm.get("strategy_type"), str):
        norm["strategy_type"] = norm["strategy_type"].lower()

    meta = norm.get("metadata")
    if isinstance(meta, dict) and isinstance(meta.get("created_at"), str):
        try:
            meta["created_at"] = datetime.fromisoformat(meta["created_at"])
        except ValueError:
            # tolerate non‑ISO strings, leave as str
            pass
        norm["metadata"] = meta

    return norm


def _build_job_from_file(cfg_path: Path):
    """
    Build a RuntimeJob directly from a JSON config using runtime_job_from_config.
    """
    raw = _load_job_cfg(cfg_path)
    cfg = _normalize_job_cfg(raw)
    return runtime_job_from_config(cfg)


@pytest.fixture()
def exec_handler() -> JobExecutionHandler:
    return JobExecutionHandler()


@pytest.mark.asyncio
async def test_execute_csv_row_job(
        tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    """
    Row strategy:
    - read a single line CSV
    - write a single line CSV
    """
    in_fp = tmp_path / "in.csv"
    pd.DataFrame([{"id": 1, "name": "Nina"}]).to_csv(in_fp, index=False)
    out_fp = tmp_path / "out.csv"

    base = _load_job_cfg(CSV_JOB_TEMPLATE)
    cfg = _with_strategy(
        _patch_filepaths(base, [("reader", in_fp), ("writer", out_fp)]), "row"
    )

    cfg_fp = tmp_path / "job_row.json"
    cfg_fp.write_text(json.dumps(cfg, default=str), encoding="utf-8")

    job = _build_job_from_file(cfg_fp)
    execution = exec_handler.execute_job(job)
    mh = exec_handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS
    out = pd.read_csv(out_fp)
    assert list(out["name"]) == ["Nina"]


@pytest.mark.asyncio
async def test_execute_csv_bulk_job(
        tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    """
    Bulk strategy:
    - read two rows
    - write two rows
    """
    in_fp = tmp_path / "in_bulk.csv"
    pd.DataFrame(
        [{"id": 2, "name": "Omar"}, {"id": 3, "name": "Lina"}]
    ).to_csv(in_fp, index=False)
    out_fp = tmp_path / "out_bulk.csv"

    base = _load_job_cfg(CSV_JOB_TEMPLATE)
    cfg = _with_strategy(
        _patch_filepaths(base, [("reader", in_fp), ("writer", out_fp)]), "bulk"
    )

    cfg_fp = tmp_path / "job_bulk.json"
    cfg_fp.write_text(json.dumps(cfg, default=str), encoding="utf-8")

    job = _build_job_from_file(cfg_fp)
    execution = exec_handler.execute_job(job)
    mh = exec_handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS
    out = pd.read_csv(out_fp).sort_values("id")
    assert list(out["name"]) == ["Omar", "Lina"]


@pytest.mark.asyncio
async def test_execute_csv_bigdata_job(
        tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    """
    BigData strategy:
    - read two rows
    - write partitioned CSVs to a folder (e.g., via Dask)
    """
    in_fp = tmp_path / "in_big.csv"
    pd.DataFrame(
        [{"id": 4, "name": "Max"}, {"id": 5, "name": "Gina"}]
    ).to_csv(in_fp, index=False)

    out_dir = tmp_path / "out_big"
    out_dir.mkdir()

    base = _load_job_cfg(CSV_JOB_TEMPLATE)
    cfg = _with_strategy(
        _patch_filepaths(base, [("reader", in_fp), ("writer", out_dir)]), "bigdata"
    )

    cfg_fp = tmp_path / "job_bigdata.json"
    cfg_fp.write_text(json.dumps(cfg, default=str), encoding="utf-8")

    job = _build_job_from_file(cfg_fp)
    execution = exec_handler.execute_job(job)
    mh = exec_handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    parts = sorted(out_dir.glob("part-*.csv"))
    assert parts, "No partition files written by BigData CSV writer."

    ddf = dd.read_csv([str(p) for p in parts], assume_missing=True, dtype=str)
    df_out = ddf.compute().sort_values("id")
    assert list(df_out["name"]) == ["Max", "Gina"]