from typing import Dict, Union, Any, Mapping
from etl_core.job_execution.runtimejob import RuntimeJob
from etl_core.persistance.configs.job_config import JobConfig
from etl_core.utils.common_helpers import (  # noqa: F401
    get_component_by_name,
    normalize_df,
)


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
