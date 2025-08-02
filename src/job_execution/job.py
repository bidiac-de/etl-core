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
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.metrics.job_metrics import JobMetrics
from uuid import uuid4
import logging
from datetime import timedelta
from src.metrics.metrics_registry import get_metrics_class
from src.components.component_registry import component_registry

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

    components: List[Component] = Field(default_factory=list)

    config: Dict[str, Any] = Field(default_factory=dict, exclude=True)
    metadata: MetaData = Field(default_factory=lambda: MetaData(), exclude=True)
    executions: List[Any] = Field(default_factory=list, exclude=True)
    _root_components: List[Component] = PrivateAttr(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _instantiate_components(cls, values: dict) -> dict:
        raw = values.get("components", [])
        built: list[Component] = []
        for comp_data in raw:
            comp_type = comp_data.get("comp_type")
            CompCls = component_registry.get(comp_type)
            if CompCls is None:
                raise ValueError(f"Unknown component type: {comp_type!r}")
            # This will now call StubComponent.build_objects (etc.)
            built.append(CompCls(**comp_data))
        values["components"] = built
        return values

    @model_validator(mode="before")
    @classmethod
    def _store_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values["config"] = values.copy()
        return values

    @model_validator(mode="after")
    def _check_names_and_wire(self) -> "Job":
        """
        Ensure each component.name is unique
        Wire up `next`/`prev` pointers by those names
        """

        # name uniqueness
        names = [c.name for c in self.components]
        dupes = {n for n in names if names.count(n) > 1}
        if dupes:
            raise ValueError(f"Duplicate component names: {sorted(dupes)}")

        # wiring
        name_map = {c.name: c for c in self.components}
        for c in self.components:
            for nxt_name in c.next:
                try:
                    nxt = name_map[nxt_name]
                except KeyError:
                    raise ValueError(f"Unknown next‐component name: {nxt_name!r}")
                c.add_next(nxt)
                nxt.add_prev(c)

        self._root_components = [c for c in self.components if not c.prev_components]

        return self

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

    _id: str
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
        self._id = str(uuid4())
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
    def id(self) -> str:
        return self._id

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

    _id: str
    _attempt_number: int = 0
    _component_metrics: Dict[str, ComponentMetrics]
    _error: str | None = None

    def __init__(
        self,
        attempt_number: int,
        execution: JobExecution,
    ) -> None:
        self._id = str(uuid4())
        if attempt_number < 1:
            raise ValueError("attempt_number must be at least 1")
        self._attempt_number = attempt_number
        self.execution = execution
        job = execution.job
        self._component_metrics = {}

        components = job.components
        self.pending: set[str] = {c.id for c in components}
        self.succeeded: set[str] = set()
        self.failed: set[str] = set()
        self.cancelled: set[str] = set()

        # metrics also keyed by name
        for comp in components:
            MetricsCls = get_metrics_class(comp.comp_type)
            metrics = MetricsCls(processing_time=timedelta(0), error_count=0)
            self._component_metrics[comp.id] = metrics

    @property
    def id(self) -> str:
        return self._id

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
