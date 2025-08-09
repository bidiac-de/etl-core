from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.job_execution.runtimejob import RuntimeJob
    from src.components.base_component import Component


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
