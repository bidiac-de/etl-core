from typing import Dict, Any, List, TYPE_CHECKING
from src.components.dataclasses import MetaData
from pydantic import BaseModel, Field, ConfigDict, NonNegativeInt, model_validator
from src.components.base_component import Component
from src.components.component_registry import component_registry
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.metrics.job_metrics import JobMetrics
from uuid import uuid4
import logging
if TYPE_CHECKING:
    from src.job_execution.job_execution_handler import JobExecutionHandler

class Job(BaseModel):
    """
    Job Objects
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

    id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="set by constructor",
        exclude=True,
    )
    name: str = Field(default="default_job_name")
    num_of_retries: NonNegativeInt = Field(default=0)
    file_logging: bool = Field(default=False)
    component_configs: List[Dict[str, Any]] = Field(default_factory=list)

    config: Dict[str, Any] = Field(default_factory=dict, exclude=True)

    # runtime-objects
    metadata: MetaData = Field(default_factory=lambda: MetaData())
    executions: List[Any] = Field(default_factory=list, exclude=True)

    components: Dict[str, Component] = Field(default_factory=dict, exclude=True)
    root_components: List[Component] = Field(default_factory=list, exclude=True)

    @model_validator(mode="before")
    @classmethod
    def _store_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        store the configuration dictionary in the job object
        """
        values["config"] = values.copy()
        return values

    @model_validator(mode="after")
    def _build_objects(self) -> "Job":
        """
        After Pydantic has set all fields, build and connect components
        """
        self._build_components()
        self._connect_components()
        return self

    def _build_components(self) -> None:
        """
        Instantiate every component named in config
        """
        comps: Dict[str, Component] = {}

        # map of temporary IDs to internal ids
        self._temp_map: Dict[str, str] = {}

        for cconf in self.config.get("component_configs", []):
            comp_type = cconf["comp_type"]
            if comp_type not in component_registry:
                raise ValueError(f"Unknown component type: {comp_type}")

            component_class = component_registry[comp_type]
            if "id" in cconf:
                user_prov_id = cconf["id"]
                component = component_class(**cconf)

                internal_id = str(uuid4())
                component.id = internal_id

                comps[component.id] = component
                self._temp_map[user_prov_id] = component.id
            else:
                component = component_class(**cconf)
                comps[component.id] = component

        self.components = comps

    def _connect_components(self) -> None:
        """
        Connect components based on the list of next components
        """
        for component in self.components.values():
            src = self.components[component.id]
            for nxt_user_prov_id in component.next:
                dest_internal = self._temp_map.get(nxt_user_prov_id)
                if dest_internal not in self.components:
                    raise ValueError(f"Unknown next-component: {nxt_user_prov_id}")

                dest = self.components[dest_internal]
                src.add_next(dest)
                dest.add_prev(src)

        self.root_components = [
            c for c in self.components.values() if not c.prev_components
        ]


class JobExecution:
    """
    class to encapsulate the execution details of a job
    """

    job: Job
    job_metrics: JobMetrics = None
    attempts: List["ExecutionAttempt"]
    file_logger: logging.Logger = None

    def __init__(self, job: Job):
        self.job = job
        self.job_metrics = JobMetrics()
        self.attempts = []


class ExecutionAttempt:
    """
    class to encapsulate the execution attempt details of a job
    """

    attempt_number: int = 0
    component_metrics: Dict[str, ComponentMetrics]
    error: str = None

    def __init__(self, attempt_number: int = 0):
        self.attempt_number = attempt_number
        self.component_metrics = {}
        self.pending: set[str] = set()
        self.succeeded: set[str] = set()
        self.failed: set[str] = set()
        self.cancelled: set[str] = set()

    def run_attempt(
            self,
            job: Job,
            handler: "JobExecutionHandler",
            max_workers: int,
    ) -> None:
        """
        Execute this attempt: schedule components in parallel and handle execution.
        """
        from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

        # initialize tracking sets
        self.pending = set(job.components.keys())

        execution = handler._local.execution

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures: dict = {}
            # submit initial ready components (roots)
            for comp in job.root_components:
                execution.file_logger.debug("Submitting '%s'", comp.name)
                metrics = self.component_metrics[comp.id]
                fut = executor.submit(
                    handler._execute_component,
                    comp,
                    None,
                    metrics,
                )
                futures[fut] = comp
                self.pending.discard(comp.id)

            while futures:
                done, _ = wait(
                    futures,
                    return_when=FIRST_COMPLETED,
                )
                for fut in done:
                    comp = futures.pop(fut)
                    handler._handle_future(fut, comp)
                    handler._schedule_next(comp, executor, futures)

        handler._mark_unrunnable()
        handler._finalize_success(job)