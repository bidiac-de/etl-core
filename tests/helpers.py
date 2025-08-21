from typing import Iterable, List, Dict, Union, Any, Mapping, Optional
from etl_core.components.base_component import Component
from etl_core.job_execution.runtimejob import RuntimeJob
from etl_core.persistance.configs.job_config import JobConfig
import pandas as pd
import json
import tempfile
import os
from pathlib import Path


def get_component_by_name(job: "RuntimeJob", name: str) -> "Component":
    """
    Look up a Component in job.components by its unique .name.

    :param job: a Job instance whose .components is a List[Component]
    :param name: the unique name to search for
    :return: the matching Component
    :raises ValueError: if no component with that name exists
    """
    for comp in job.components:
        if comp.name == name:
            return comp
    raise ValueError(f"No component named {name!r} found in job.components")


def _wire_components(components: Iterable[Component]) -> None:
    """
    Wire in-memory component objects using their `next` names:
      - sets `comp.next_components`
      - appends to `nxt.prev_components`
    Assumes names are unique and `next` names were validated.
    """
    comps: List[Component] = list(components)
    name_map: Dict[str, Component] = {c.name: c for c in comps}

    for comp in comps:
        # Map configured next names -> actual component objects
        try:
            next_objs = [name_map[n] for n in comp.next]
        except KeyError as exc:
            raise ValueError(f"Unknown next-component name: {exc.args[0]!r}") from exc

        comp.next_components = next_objs
        for nxt in next_objs:
            nxt.add_prev(comp)


def runtime_job_from_config(
    cfg_like: Union[Mapping[str, Any], JobConfig],
) -> RuntimeJob:
    """
    Build a RuntimeJob directly from a config (no DB involved).

    Steps:
      1) Validate/inflate via JobConfig (also checks duplicate names & next refs).
      2) Construct RuntimeJob with components/metadata.
      3) Wire next/prev relationships in-memory.

    Returns:
        RuntimeJob: ready for execution in tests.
    """
    cfg = cfg_like if isinstance(cfg_like, JobConfig) else JobConfig(**cfg_like)

    # Create the runtime job with components present so the job-level validator
    # assigns strategies to each component.
    job = RuntimeJob(
        name=cfg.name,
        num_of_retries=cfg.num_of_retries,
        file_logging=cfg.file_logging,
        strategy_type=cfg.strategy_type,
        components=list(cfg.components),  # copy for safety
        metadata=cfg.metadata_.model_dump(),
    )

    # Wire component relationships in-memory
    _wire_components(job.components)
    return job


def detail_message(payload: Dict[str, Any]) -> str:
    detail = payload.get("detail")
    if isinstance(detail, str):
        return detail
    if isinstance(detail, dict):
        return str(detail.get("message", ""))
    return ""


def normalize_df(
    df: pd.DataFrame, sort_cols: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Stable sort + reset index for robust equality checks across engines.
    Ensures comparisons are not sensitive to ordering.
    """
    if sort_cols is None:
        sort_cols = list(df.columns)
    sort_cols = [c for c in sort_cols if c in df.columns]
    if sort_cols:
        df = df.sort_values(by=sort_cols, kind="mergesort")
    return df.reset_index(drop=True)


# CLI-specific helpers
def create_temp_job_config(config_data: Dict[str, Any]) -> Path:
    """
    Create a temporary JSON file with job configuration for CLI testing.

    :param config_data: Job configuration dictionary
    :return: Path to temporary file
    """
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    json.dump(config_data, temp_file)
    temp_file.close()
    return Path(temp_file.name)


def cleanup_temp_file(file_path: Path) -> None:
    """
    Clean up temporary file created for CLI testing.

    :param file_path: Path to temporary file
    """
    try:
        if file_path.exists():
            os.unlink(file_path)
    except OSError:
        pass  # File might already be deleted


def get_sample_job_config() -> Dict[str, Any]:
    """
    Get a sample job configuration for testing.

    :return: Sample job configuration dictionary
    """
    return {
        "name": "test_job",
        "num_of_retries": 3,
        "file_logging": True,
        "strategy_type": "row",
        "components": [],
        "metadata_": {
            "description": "Test job for CLI testing",
            "tags": ["test", "cli"],
        },
    }
