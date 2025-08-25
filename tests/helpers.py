from typing import List, Dict, Union, Any, Mapping, Optional
from etl_core.components.base_component import Component
from etl_core.job_execution.runtimejob import RuntimeJob
from etl_core.persistance.configs.job_config import JobConfig
import pandas as pd


def get_component_by_name(job: "RuntimeJob", name: str) -> "Component":
    """Look up a Component in job.components by its unique .name."""
    for comp in job.components:
        if comp.name == name:
            return comp
    raise ValueError(f"No component named {name!r} found in job.components")


def runtime_job_from_config(
    cfg_like: Union[Mapping[str, Any], JobConfig],
) -> RuntimeJob:
    """
    Build a RuntimeJob directly from a config (no DB involved).

    Steps:
      1) Validate/inflate via JobConfig.
      2) Construct RuntimeJob with components/metadata.

    NOTE: Do NOT wire next/prev here. RuntimeJob's validator wires using `routes`.
    """
    cfg = cfg_like if isinstance(cfg_like, JobConfig) else JobConfig(**cfg_like)

    job = RuntimeJob(
        name=cfg.name,
        num_of_retries=cfg.num_of_retries,
        file_logging=cfg.file_logging,
        strategy_type=cfg.strategy_type,
        components=list(cfg.components),  # copy for safety
        metadata=cfg.metadata_.model_dump(),
    )
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
