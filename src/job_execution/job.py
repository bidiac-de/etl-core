from typing import Dict, Any, List, TYPE_CHECKING
from src.components.dataclasses import MetaData
from pydantic import (
    BaseModel,
    Field,
    ConfigDict,
    NonNegativeInt,
    model_validator,
    field_validator,
    PrivateAttr,
)
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

    model_config = ConfigDict(
        arbitrary_types_allowed=True, extra="ignore", validate_assignment=True
    )

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
    _components: Dict[str, Component] = PrivateAttr(default_factory=dict)
    _root_components: List[Component] = PrivateAttr(default_factory=list)

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

        self._components = comps

    @field_validator("id", mode="before")
    @classmethod
    def validate_id(cls, value):
        """
        Id is read-only and should not be set manually
        """
        raise ValueError("Id is read-only and should not be set manually.")

    @field_validator("name", mode="before")
    @classmethod
    def validate_name(cls, value: str) -> str:
        """
        Validate that the name is a non-empty string.
        """
        if not isinstance(value, str) or not value.strip():
            raise ValueError("Job name must be a non-empty string.")
        return value.strip()

    @field_validator("num_of_retries", mode="before")
    @classmethod
    def validate_num_of_retries(cls, value: int) -> NonNegativeInt:
        """
        Validate that the number of retries is a non-negative integer.
        """
        if not isinstance(value, int) or value < 0:
            raise ValueError("Number of retries must be a non-negative integer.")
        return NonNegativeInt(value)

    @field_validator("file_logging", mode="before")
    @classmethod
    def validate_file_logging(cls, value: bool) -> bool:
        """
        Validate that file_logging is a boolean.
        """
        if not isinstance(value, bool):
            raise ValueError("File logging must be a boolean value.")
        return value

    @field_validator("component_configs", mode="before")
    @classmethod
    def validate_component_configs(
        cls, value: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Validate that component_configs is a list of dictionaries."""
        if not isinstance(value, list):
            raise ValueError("Component configs must be a list.")
        for item in value:
            if not isinstance(item, dict):
                raise ValueError("Each component config must be a dictionary.")
        return value

    @field_validator("metadata", mode="before")
    @classmethod
    def _cast_metadata(cls, v: MetaData | dict) -> MetaData:
        if isinstance(v, MetaData):
            return v
        if isinstance(v, dict):
            # let MetaData do its own validation on timestamps, ids, etc.
            return MetaData(**v)
        raise TypeError(f"metadata must be MetaData or dict, got {type(v).__name__}")

    @field_validator("executions", mode="before")
    @classmethod
    def validate_executions(cls, value: List[Any]) -> List[Any]:
        """
        Validate that executions is a list.
        """
        if not isinstance(value, list):
            raise ValueError("Executions must be a list.")
        return value

    @property
    def components(self) -> Dict[str, Component]:
        return self._components

    @property
    def root_components(self) -> List[Component]:
        return self._root_components

    def _connect_components(self) -> None:
        for component in self._components.values():
            src = self._components[component.id]
            for nxt_user_prov_id in component.next:
                dest_internal = self._temp_map.get(nxt_user_prov_id)
                if dest_internal not in self._components:
                    raise ValueError(f"Unknown next-component: {nxt_user_prov_id}")
                dest = self._components[dest_internal]
                src.add_next(dest)
                dest.add_prev(src)

        self._root_components = [
            c for c in self._components.values() if not c._prev_components
        ]


class JobExecution:
    """
    Encapsulates the execution details of a job
    """

    _job: Job
    _job_metrics: JobMetrics = None
    _attempts: List["ExecutionAttempt"]
    _file_logger: logging.Logger = None

    def __init__(
        self,
        job: Job,
        number_of_attempts: int,
        handler: "JobExecutionHandler",
    ):
        self._job = job
        self._job_metrics = JobMetrics()
        self._attempts = []
        if number_of_attempts < 1:
            raise ValueError("number_of_attempts must be at least 1")
        self._number_of_attempts = number_of_attempts

        self.handler = handler
        handler.running_executions.append(self)
        handler.job_information_handler.logging_handler.update_job_name(job.name)
        self._file_logger = handler.job_information_handler.logging_handler.logger
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
    def job_metrics(self) -> JobMetrics:
        return self._job_metrics

    @property
    def number_of_attempts(self) -> int:
        return self._number_of_attempts

    @property
    def job(self) -> Job:
        return self._job

    @property
    def attempts(self) -> List["ExecutionAttempt"]:
        return self._attempts

    @property
    def file_logger(self) -> logging.Logger:
        return self._file_logger


class ExecutionAttempt:
    """
    Encapsulates the details of a single execution attempt
    """

    _attempt_number: int = 0
    _component_metrics: Dict[str, ComponentMetrics]
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
        self._component_metrics = {}
        self.pending: set[str] = set()
        self.succeeded: set[str] = set()
        self.failed: set[str] = set()
        self.cancelled: set[str] = set()

        for comp in job._components.values():
            MetricsCls = get_metrics_class(comp.comp_type)
            metrics = MetricsCls(processing_time=timedelta(0), error_count=0)
            self._component_metrics[comp.id] = metrics

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

        self.pending = set(job._components.keys())
        futures: dict = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for comp in job._root_components:
                execution.file_logger.debug("Submitting '%s'", comp.name)
                metrics = self.component_metrics[comp.id]
                fut = executor.submit(handler.execute_component, comp, None, metrics)
                futures[fut] = comp
                self.pending.discard(comp.id)

            while futures:
                done, _ = wait(futures, return_when=FIRST_COMPLETED)
                for fut in done:
                    comp = futures.pop(fut)
                    handler.handle_future(fut, comp, self, execution)
                    handler.schedule_next(comp, executor, futures, self, execution)

        handler.mark_unrunnable(self, execution)
        handler.finalize_success(execution, self)

    @property
    def attempt_number(self) -> int:
        return self._attempt_number

    @property
    def error(self) -> str | None:
        return self._error

    @error.setter
    def error(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("error must be a string")
        self._error = value

    @property
    def component_metrics(self) -> Dict[str, ComponentMetrics]:
        return self._component_metrics
