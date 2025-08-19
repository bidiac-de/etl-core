from typing import Iterable, List, Dict, Union, Any, Mapping
from etl_core.components.base_component import Component
from etl_core.job_execution.runtimejob import RuntimeJob
from etl_core.persistance.configs.job_config import JobConfig


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
