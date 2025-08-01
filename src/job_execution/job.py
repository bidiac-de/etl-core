from typing import Dict, Any, List, TYPE_CHECKING
from src.components.dataclasses import MetaData
from pydantic import BaseModel, Field, ConfigDict, NonNegativeInt, model_validator
from src.components.base_component import Component
from src.components.component_registry import component_registry
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.metrics.job_metrics import JobMetrics
from uuid import uuid4
import logging
from datetime import timedelta
from src.metrics.metrics_registry import get_metrics_class

logger = logging.getLogger("job.ExecutionHandler")

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
    metadata: MetaData = Field(default_factory=lambda: MetaData(), exclude=True)
    executions: List[Any] = Field(default_factory=list, exclude=True)
    components: Dict[str, Component] = Field(default_factory=dict, exclude=True)
    root_components: List[Component] = Field(default_factory=list, exclude=True)

    @model_validator(mode="before")
    @classmethod
    def _store_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values["config"] = values.copy()
        return values

    @model_validator(mode="after")
    def _build_objects(self) -> "Job":
        self._build_components()
        self._connect_components()
        return self

    def _build_components(self) -> None:
        comps: Dict[str, Component] = {}
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
                comps[internal_id] = component
                self._temp_map[user_prov_id] = internal_id
            else:
                component = component_class(**cconf)
                comps[component.id] = component

        self.components = comps

    def _connect_components(self) -> None:
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
    Encapsulates the execution details of a job
    """

    job: Job
    job_metrics: JobMetrics = None
    attempts: List["ExecutionAttempt"]
    file_logger: logging.Logger = None

    def __init__(
        self,
        job: Job,
        number_of_attempts: int,
        handler: "JobExecutionHandler",
    ):
        self.job = job
        self.job_metrics = JobMetrics()
        self.attempts = []
        if number_of_attempts < 1:
            raise ValueError("number_of_attempts must be at least 1")
        self._number_of_attempts = number_of_attempts

        self.handler = handler
        handler.running_executions.append(self)
        handler.job_information_handler.logging_handler.update_job_name(job.name)
        self.file_logger = handler.job_information_handler.logging_handler.logger
        self.job.executions.append(self)
        logger.info("Starting execution of job '%s'", job.name)

    def create_attempt(self) -> "ExecutionAttempt":
        """
        Create and register a new execution attempt.
        """
        attempt_number = len(self.attempts) + 1
        attempt = ExecutionAttempt(attempt_number=attempt_number, execution=self)
        self.attempts.append(attempt)
        return attempt

    @property
    def number_of_attempts(self) -> int:
        return self._number_of_attempts

    @number_of_attempts.setter
    def number_of_attempts(self, value: int) -> None:
        if not isinstance(value, int) or value < 1:
            raise ValueError("number_of_attempts must be a non-negative integer ≥ 1")
        self._number_of_attempts = value


class ExecutionAttempt:
    """
    Encapsulates the details of a single execution attempt
    """

    _attempt_number: int = 0
    component_metrics: Dict[str, ComponentMetrics]
    _error: str | None = None

    def __init__(
        self,
        attempt_number: int,
        execution: JobExecution,
    ) -> None:
        if attempt_number < 1:
            raise ValueError("attempt_number must be at least 1")
        self._attempt_number = attempt_number
        self.execution = execution
        job = execution.job
        self.component_metrics = {}
        self.pending: set[str] = set()
        self.succeeded: set[str] = set()
        self.failed: set[str] = set()
        self.cancelled: set[str] = set()

        for comp in job.components.values():
            MetricsCls = get_metrics_class(comp.comp_type)
            metrics = MetricsCls(processing_time=timedelta(0), error_count=0)
            self.component_metrics[comp.id] = metrics

    def run_attempt(
        self,
        max_workers: int,
    ) -> None:
        """
        Execute this attempt: schedule components in parallel and handle execution.
        """
        from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

        execution = self.execution
        job = execution.job
        handler = execution.handler

        self.pending = set(job.components.keys())
        futures: dict = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for comp in job.root_components:
                execution.file_logger.debug("Submitting '%s'", comp.name)
                metrics = self.component_metrics[comp.id]
                fut = executor.submit(handler._execute_component, comp, None, metrics)
                futures[fut] = comp
                self.pending.discard(comp.id)

            while futures:
                done, _ = wait(futures, return_when=FIRST_COMPLETED)
                for fut in done:
                    comp = futures.pop(fut)
                    handler._handle_future(fut, comp, self, execution)
                    handler._schedule_next(comp, executor, futures, self, execution)

        handler._mark_unrunnable(self, execution)
        handler._finalize_success(execution, self)

    @property
    def attempt_number(self) -> int:
        return self._attempt_number

    @attempt_number.setter
    def attempt_number(self, value: int) -> None:
        if not isinstance(value, int) or value < 1:
            raise ValueError("attempt_number must be a non-negative integer ≥ 1")
        self._attempt_number = value

    @property
    def error(self) -> str | None:
        return self._error

    @error.setter
    def error(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("error must be a string")
        self._error = value
