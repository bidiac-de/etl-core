import json
import pytest
import pandas as pd
import dask.dataframe as dd
from pathlib import Path

from etl_core.singletons import job_handler
from etl_core.components.runtime_state import RuntimeState


@pytest.mark.asyncio
async def test_execute_json_row_job(tmp_path, schema_definition):
    in_fp = tmp_path / "in.json"
    pd.DataFrame([{"id": 1, "name": "Nina"}]).to_json(in_fp, orient="records")

    out_fp = tmp_path / "out.json"

    job_cfg = {
        "name": "JsonRowJob",
        "num_of_retries": 0,
        "file_logging": False,
        "strategy_type": "row",
        "components": [
            {"name": "reader", "comp_type": "read_json", "filepath": str(in_fp), "schema": schema_definition},
            {"name": "writer", "comp_type": "write_json", "filepath": str(out_fp), "schema": schema_definition},
        ],
    }
    cfg_fp = tmp_path / "row_job.json"
    cfg_fp.write_text(json.dumps(job_cfg, default=str))

    # Job über Singleton ausführen
    job = job_handler.load_from_file(cfg_fp)
    execution = job_handler.execute(job)

    assert job_handler.metrics.get_job_metrics(execution.id).status == RuntimeState.SUCCESS
    out = pd.read_json(out_fp, orient="records")
    assert list(out["name"]) == ["Nina"]


@pytest.mark.asyncio
async def test_execute_json_bulk_job(tmp_path, schema_definition):
    in_fp = tmp_path / "in_bulk.json"
    df_in = pd.DataFrame([{"id": 2, "name": "Omar"}, {"id": 3, "name": "Lina"}])
    df_in.to_json(in_fp, orient="records")

    out_fp = tmp_path / "out_bulk.json"

    job_cfg = {
        "name": "JsonBulkJob",
        "strategy_type": "bulk",
        "components": [
            {"name": "reader", "comp_type": "read_json", "filepath": str(in_fp), "schema": schema_definition},
            {"name": "writer", "comp_type": "write_json", "filepath": str(out_fp), "schema": schema_definition},
        ],
    }
    cfg_fp = tmp_path / "bulk_job.json"
    cfg_fp.write_text(json.dumps(job_cfg, default=str))

    job = job_handler.load_from_file(cfg_fp)
    execution = job_handler.execute(job)

    assert job_handler.metrics.get_job_metrics(execution.id).status == RuntimeState.SUCCESS
    out = pd.read_json(out_fp, orient="records").sort_values("id")
    assert list(out["name"]) == ["Omar", "Lina"]


@pytest.mark.asyncio
async def test_execute_json_bigdata_job(tmp_path, schema_definition):
    in_fp = tmp_path / "in_big.jsonl"
    df_in = pd.DataFrame([{"id": 4, "name": "Max"}, {"id": 5, "name": "Gina"}])
    df_in.to_json(in_fp, orient="records", lines=True)

    out_dir = tmp_path / "out_big"
    out_dir.mkdir()

    job_cfg = {
        "name": "JsonBigDataJob",
        "strategy_type": "bigdata",
        "components": [
            {"name": "reader", "comp_type": "read_json", "filepath": str(in_fp), "schema": schema_definition},
            {"name": "writer", "comp_type": "write_json", "filepath": str(out_dir), "schema": schema_definition},
        ],
    }
    cfg_fp = tmp_path / "bigdata_job.json"
    cfg_fp.write_text(json.dumps(job_cfg, default=str))

    job = job_handler.load_from_file(cfg_fp)
    execution = job_handler.execute(job)

    assert job_handler.metrics.get_job_metrics(execution.id).status == RuntimeState.SUCCESS
    parts = sorted(out_dir.glob("part-*.jsonl"))
    assert parts, "No partition files written"

    ddf_out = dd.read_json([str(p) for p in parts], lines=True, blocksize="64MB")
    df_out = ddf_out.compute().sort_values("id")
    assert list(df_out["name"]) == ["Max", "Gina"]