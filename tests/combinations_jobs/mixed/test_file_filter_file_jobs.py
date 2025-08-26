from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import dask.dataframe as dd
import pandas as pd
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


def _load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _parse_json(text: str) -> Dict[str, Any]:
    return json.loads(text)


def _normalize_components(components: List[Dict[str, Any]]) -> None:
    """
    Normalize components: comp_type/type, default description/options.
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
    cfg: Dict[str, Any], in_fp: Path, out_path: Path
) -> Dict[str, Any]:
    """
    Replace placeholders and set filepaths on reader/writer based on comp_type.
    Avoid JSON string round-trip to prevent invalid escapes on Windows paths.
    """
    components = cfg.get("components", [])
    for comp in components:
        # Replace explicit placeholders
        fp = comp.get("filepath")
        if fp == "__IN__":
            comp["filepath"] = str(in_fp)
        elif fp == "__OUT__":
            comp["filepath"] = str(out_path)

    return cfg


def _render_job_cfg(cfg_path: Path, in_fp: Path, out_path: Path) -> Dict[str, Any]:
    cfg = _parse_json(_load_text(cfg_path))
    _normalize_components(cfg.get("components", []))
    return _inject_filepaths(cfg, in_fp, out_path)


def _data_path(*rel: str) -> Path:
    """
    Find a known test data file (CSV/JSON/Excel) by walking up a few levels.
    This makes the test robust to directory layout differences.
    """
    here = Path(__file__).parent
    for _ in range(6):
        candidate = here.joinpath(*rel)
        if candidate.exists():
            return candidate
        here = here.parent
    raise FileNotFoundError(f"Could not locate test data: {'/'.join(rel)}")


def _read_output(writer_type: str, strategy: str, out: Path) -> pd.DataFrame:
    """
    Read output into a pandas DataFrame regardless of writer type/strategy.
    """
    if writer_type == "write_csv":
        return pd.read_csv(out)

    if writer_type == "write_excel":
        return pd.read_excel(out)

    if writer_type == "write_json":
        if strategy == "row":
            return pd.read_json(out, lines=True)
        if strategy == "bulk":
            return pd.read_json(out)
        if strategy == "bigdata":
            # out is a directory with part-*.jsonl
            parts = sorted(out.glob("part-*.jsonl"))
            if not parts:
                # Some implementations write just *.jsonl
                parts = sorted(out.glob("*.jsonl"))
            if not parts:
                raise AssertionError("No NDJSON part files written.")
            ddf = dd.read_json([str(p) for p in parts], lines=True, blocksize="64MB")
            return ddf.compute()
        raise AssertionError(f"Unknown json strategy: {strategy}")

    raise AssertionError(f"Unknown writer type: {writer_type}")


def _cfg_dir() -> Path:
    # Put the JSON configs right next to this test file
    return Path(__file__).with_suffix("").parent


def _cfg(path_name: str) -> Path:
    return _cfg_dir() / path_name


@pytest.fixture()
def exec_handler() -> JobExecutionHandler:
    return JobExecutionHandler()


CSV_VALID = _data_path("tests", "components", "data", "csv", "test_data.csv")
JSON_VALID_NDJSON = _data_path("tests", "components", "data", "json", "testdata.jsonl")
JSON_VALID_ARRAY = _data_path("tests", "components", "data", "json", "testdata.json")
EXCEL_VALID = _data_path("tests", "components", "data", "excel", "test_data.xlsx")


def test_csv_row_filter_to_csv(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("csv_row_filter_csv.json")
    out_fp = tmp_path / "out_row.csv"
    cfg = _render_job_cfg(cfg_path, CSV_VALID, out_fp)
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.SUCCESS
    )

    out = _read_output("write_csv", cfg["strategy_type"], out_fp).sort_values("id")
    assert list(out["name"]) == ["Alice", "Charlie"]  # filtered: drop Bob


def test_csv_bigdata_filter_to_csv(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("csv_bigdata_filter_csv.json")
    out_fp = tmp_path / "out_big.csv"
    cfg = _render_job_cfg(cfg_path, CSV_VALID, out_fp)
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.SUCCESS
    )

    ddf = dd.read_csv(str(out_fp), assume_missing=True, dtype=str)
    out = ddf.compute().sort_values("id")
    assert list(out["name"]) == ["Alice", "Charlie"]  # contains 'i'


def test_json_row_filter_to_csv(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("json_row_filter_csv.json")
    out_fp = tmp_path / "out_row.csv"
    cfg = _render_job_cfg(cfg_path, JSON_VALID_NDJSON, out_fp)
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.SUCCESS
    )

    out = _read_output("write_csv", cfg["strategy_type"], out_fp).sort_values("id")
    assert list(out["name"]) == ["Alice", "Charlie"]


def test_json_bulk_filter_to_json(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("json_bulk_filter_json.json")
    out_fp = tmp_path / "out_bulk.json"
    cfg = _render_job_cfg(cfg_path, JSON_VALID_ARRAY, out_fp)
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.SUCCESS
    )

    out = _read_output("write_json", cfg["strategy_type"], out_fp).sort_values("id")
    assert list(out["name"]) == ["Bob", "Charlie"]  # id >= 2


def test_json_bigdata_filter_to_json_dir(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("json_bigdata_filter_json.json")
    out_dir = tmp_path / "out_big_ndjson"
    out_dir.mkdir(parents=True, exist_ok=True)

    cfg = _render_job_cfg(cfg_path, JSON_VALID_NDJSON, out_dir)
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.SUCCESS
    )

    out = _read_output("write_json", cfg["strategy_type"], out_dir).sort_values("id")
    assert list(out["name"]) == ["Alice", "Charlie"]  # drop Bob via OR-rule


def test_excel_bulk_filter_to_excel(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("excel_bulk_filter_excel.json")
    out_fp = tmp_path / "out_bulk.xlsx"
    cfg = _render_job_cfg(cfg_path, EXCEL_VALID, out_fp)
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.SUCCESS
    )

    out = _read_output("write_excel", cfg["strategy_type"], out_fp).sort_values("id")
    assert list(out["name"]) == ["Alice", "Charlie"]


def test_excel_bulk_filter_to_csv(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("excel_bulk_filter_csv.json")
    out_fp = tmp_path / "out_bulk.csv"
    cfg = _render_job_cfg(cfg_path, EXCEL_VALID, out_fp)
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.SUCCESS
    )

    out = _read_output("write_csv", cfg["strategy_type"], out_fp).sort_values("id")
    assert list(out["name"]) == ["Alice", "Charlie"]


def test_csv_bulk_filter_to_excel(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("csv_bulk_filter_excel.json")
    out_fp = tmp_path / "out_bulk.xlsx"
    cfg = _render_job_cfg(cfg_path, CSV_VALID, out_fp)
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.SUCCESS
    )

    out = _read_output("write_excel", cfg["strategy_type"], out_fp).sort_values("id")
    assert list(out["name"]) == ["Alice", "Charlie"]


def test_excel_bulk_filter_to_json(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    cfg_path = _cfg("excel_bulk_filter_json.json")
    out_fp = tmp_path / "out_bulk.json"
    cfg = _render_job_cfg(cfg_path, EXCEL_VALID, out_fp)
    job = runtime_job_from_config(cfg)

    run = exec_handler.execute_job(job)
    assert (
        exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
        == RuntimeState.SUCCESS
    )

    out = _read_output("write_json", cfg["strategy_type"], out_fp).sort_values("id")
    assert list(out["name"]) == ["Alice", "Charlie"]
