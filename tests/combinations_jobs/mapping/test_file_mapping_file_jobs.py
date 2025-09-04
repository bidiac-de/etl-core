from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

import dask.dataframe as dd
import pytest

# Ensure CSV and mapping components are registered
from etl_core.components.file_components.csv.read_csv import (  # noqa: F401
    ReadCSV,
)
from etl_core.components.file_components.csv.write_csv import (  # noqa: F401
    WriteCSV,
)
from etl_core.components.data_operations.schema_mapping.schema_mapping_component import (  # noqa: E501,F401
    SchemaMappingComponent,
)

from etl_core.components.runtime_state import RuntimeState
from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from tests.config_helpers import (
    render_job_cfg_with_filepaths,
    read_output,
    data_path,
)
from tests.helpers import runtime_job_from_config


def _cfg_dir() -> Path:
    return Path(__file__).with_suffix("").parent


def _cfg(name: str) -> Path:
    return _cfg_dir() / name


@pytest.fixture()
def exec_handler() -> JobExecutionHandler:
    return JobExecutionHandler()


CSV_VALID = data_path("tests", "components", "data", "csv", "test_data.csv")


def _load_cfg_and_apply_paths(
    cfg_file: str, replacements: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Helper: load the JSON config and replace filepaths. This helper injects
    paths by *component type* (comp_type), not by component name.
    """
    return render_job_cfg_with_filepaths(_cfg(cfg_file), replacements)


def _set_component_filepath_by_name(cfg: Dict[str, Any], name: str, fp: Path) -> None:
    for comp in cfg.get("components", []):
        if comp.get("name") == name:
            comp["filepath"] = str(fp)
            return
    raise AssertionError(f"Component named {name!r} not found in config.")


def test_csv_bulk_map_fanout_to_two_csvs(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    """
    One CSV -> SchemaMapping fan-out -> two CSV sinks.
    """
    cfg = _load_cfg_and_apply_paths(
        "csv_bulk_map_fanout_csv.json",
        {
            "read_csv": CSV_VALID,
        },
    )
    _set_component_filepath_by_name(cfg, "writer_A", tmp_path / "out_A.csv")
    _set_component_filepath_by_name(cfg, "writer_B", tmp_path / "out_B.csv")

    job = runtime_job_from_config(cfg)
    run = exec_handler.execute_job(job)

    status = exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
    print(run.attempts[0].error)
    assert status == RuntimeState.SUCCESS

    out_a = read_output(
        "write_csv", cfg["strategy_type"], Path(cfg["components"][-2]["filepath"])
    ).sort_values("uid", ignore_index=True)
    out_b = read_output(
        "write_csv", cfg["strategy_type"], Path(cfg["components"][-1]["filepath"])
    ).sort_values("id", ignore_index=True)

    assert list(out_a.columns) == ["uid", "uname"]
    assert list(out_b.columns) == ["id", "name"]
    assert len(out_a) == 3 and len(out_b) == 3
    assert set(out_a["uname"]) == {"Alice", "Bob", "Charlie"}


@pytest.mark.parametrize(
    ("cfg_name", "outfile"),
    [
        ("csv_bulk_join_inner_csv.json", "inner.csv"),
        ("csv_bulk_join_left_csv.json", "left.csv"),
        ("csv_bulk_join_right_csv.json", "right.csv"),
        ("csv_bulk_join_outer_csv.json", "outer.csv"),
    ],
)
def test_csv_bulk_self_join_modes_to_csv(
    tmp_path: Path,
    exec_handler: JobExecutionHandler,
    cfg_name: str,
    outfile: str,
) -> None:
    """
    Two readers pointing to the same CSV; join by id in four modes.
    Result should have id, name_x, name_y for these configs.
    """
    out_fp = tmp_path / outfile
    cfg = _load_cfg_and_apply_paths(
        cfg_name,
        {
            # both readers get the same input via comp_type mapping
            "read_csv": CSV_VALID,
            # single writer, so comp_type mapping works fine
            "write_csv": out_fp,
        },
    )

    job = runtime_job_from_config(cfg)
    run = exec_handler.execute_job(job)

    status = exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
    assert status == RuntimeState.SUCCESS

    out = read_output("write_csv", cfg["strategy_type"], out_fp)
    out = out.sort_values("id").reset_index(drop=True)

    assert list(out.columns) == ["id", "name_x", "name_y"]
    assert len(out) == 3
    assert set(out["name_x"]) == {"Alice", "Bob", "Charlie"}
    assert set(out["name_y"]) == {"Alice", "Bob", "Charlie"}


def test_csv_bigdata_self_join_inner_to_single_csv(
    tmp_path: Path, exec_handler: JobExecutionHandler
) -> None:
    """
    Bigdata/dask self-join on id
    """
    out_file = tmp_path / "big_out.csv"
    cfg = _load_cfg_and_apply_paths(
        "csv_bigdata_join_inner_csv.json",
        {
            "read_csv": CSV_VALID,
            "write_csv": out_file,
        },
    )

    job = runtime_job_from_config(cfg)
    run = exec_handler.execute_job(job)

    status = exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
    assert status == RuntimeState.SUCCESS

    # Read the single CSV file with dask
    assert out_file.is_file(), f"Expected single file at {out_file!s}"
    ddf = dd.read_csv([str(out_file)], assume_missing=True, dtype=str)
    out = ddf.compute().sort_values("id").reset_index(drop=True)

    assert list(out.columns) == ["id", "name_x", "name_y"]
    assert len(out) == 3
