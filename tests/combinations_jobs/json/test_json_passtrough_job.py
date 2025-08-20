import json
import pandas as pd
import dask.dataframe as dd
from pathlib import Path

from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from etl_core.components.runtime_state import RuntimeState
from tests.helpers import runtime_job_from_config


def _load_cfg(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _make_job(cfg_path: Path, in_path: Path, out_path: Path):
    cfg = _load_cfg(cfg_path)
    for comp in cfg["components"]:
        if comp["comp_type"] == "read_json":
            comp["filepath"] = str(in_path)
        if comp["comp_type"] == "write_json":
            comp["filepath"] = str(out_path)
    return runtime_job_from_config(cfg)


def test_execute_json_row_job(tmp_path: Path):
    # Row-Streaming: NDJSON (.jsonl)
    in_fp = tmp_path / "in.jsonl"
    rows = [{"id": 1, "name": "Nina"}, {"id": 2, "name": "Max"}]
    in_fp.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")

    out_fp = tmp_path / "out.jsonl"

    cfg_path = Path(__file__).parent / "job_config_row.json"
    job = _make_job(cfg_path, in_fp, out_fp)

    handler = JobExecutionHandler()
    execution = handler.execute_job(job)

    assert (
        handler.job_info.metrics_handler.get_job_metrics(execution.id).status
        == RuntimeState.SUCCESS
    )
    out = pd.read_json(out_fp, lines=True)
    assert list(out["name"]) == ["Nina", "Max"]


def test_execute_json_bulk_job(tmp_path: Path):
    # Bulk: JSON-Array in/out (.json)
    in_fp = tmp_path / "in_bulk.json"
    df_in = pd.DataFrame([{"id": 2, "name": "Omar"}, {"id": 3, "name": "Lina"}])
    df_in.to_json(in_fp, orient="records")

    out_fp = tmp_path / "out_bulk.json"

    cfg_path = Path(__file__).parent / "job_config_bulk.json"
    job = _make_job(cfg_path, in_fp, out_fp)

    handler = JobExecutionHandler()
    execution = handler.execute_job(job)

    assert (
        handler.job_info.metrics_handler.get_job_metrics(execution.id).status
        == RuntimeState.SUCCESS
    )
    out = pd.read_json(out_fp, orient="records").sort_values("id")
    assert list(out["name"]) == ["Omar", "Lina"]


def test_execute_json_bigdata_job(tmp_path: Path):
    # BigData: NDJSON input (.jsonl), partitioned NDJSON output (directory)
    in_fp = tmp_path / "in_big.jsonl"
    records = [{"id": 4, "name": "Max"}, {"id": 5, "name": "Gina"}]
    in_fp.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")

    out_dir = tmp_path / "out_big"
    out_dir.mkdir(parents=True, exist_ok=True)

    cfg_path = Path(__file__).parent / "job_config_bigdata.json"
    job = _make_job(cfg_path, in_fp, out_dir)

    handler = JobExecutionHandler()
    execution = handler.execute_job(job)

    assert (
        handler.job_info.metrics_handler.get_job_metrics(execution.id).status
        == RuntimeState.SUCCESS
    )

    parts = sorted(out_dir.glob("part-*.jsonl"))
    assert parts, "No partition files written."

    ddf_out = dd.read_json([str(p) for p in parts], lines=True, blocksize="64MB")
    df_out = ddf_out.compute().sort_values("id")
    assert list(df_out["name"]) == ["Max", "Gina"]
