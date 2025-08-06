from typing import List
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
from collections import Counter

from src.components.base_component import Component, get_strategy, StrategyType
from uuid import uuid4
import logging
from src.components.component_registry import component_registry

logger = logging.getLogger("job.ExecutionHandler")


class Job(BaseModel):
    """
    Job Objects
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True, extra="ignore", validate_assignment=True
    )

    _id: str = PrivateAttr(default_factory=lambda: str(uuid4()))
    name: str = Field(default="default_job_name")
    num_of_retries: NonNegativeInt = Field(default=0)
    file_logging: bool = Field(default=False)
    strategy_type: StrategyType = Field(default=StrategyType.ROW)

    components: List[Component] = Field(default_factory=list)

    metadata: MetaData = Field(default_factory=lambda: MetaData(), exclude=True)

    @model_validator(mode="before")
    @classmethod
    def _instantiate_components(cls, values: dict) -> dict:
        raw = values.get("components", [])
        built: list[Component] = []
        for comp_data in raw:
            comp_type = comp_data.get("comp_type")
            comp_cls = component_registry.get(comp_type)
            if comp_cls is None:
                valid_types = list(component_registry.keys())
                raise ValueError(
                    f"Unknown component type: {comp_type!r}. "
                    f"Valid types are: {valid_types}"
                )
            built.append(comp_cls(**comp_data))
        values["components"] = built
        return values

    @model_validator(mode="after")
    def _check_names_and_wire(self) -> "Job":
        # check names for duplicates
        counts = Counter(c.name for c in self.components)
        dupes = [name for name, cnt in counts.items() if cnt > 1]
        if dupes:
            raise ValueError(f"Duplicate component names: {sorted(dupes)}")

        # create mapping
        name_map = {c.name: c for c in self.components}

        # wire up components using comprehension
        for comp in self.components:
            try:
                next_objs = [name_map[n] for n in comp.next]
            except KeyError as e:
                raise ValueError(f"Unknown next‐component name: {e.args[0]!r}")
            comp.next_components = next_objs
            for nxt in next_objs:
                nxt.prev_components.append(comp)

        return self

    @model_validator(mode="after")
    def _assign_strategies(self) -> "Job":
        """
        After wiring, give every component the Job’s strategy.
        """
        for comp in self.components:
            # override whatever was on the component; use job-level strategy_type
            comp.strategy = get_strategy(self.strategy_type)

        return self

    @field_validator("name", "strategy_type", mode="before")
    @classmethod
    def _validate_non_empty_string(cls, value: str) -> str:
        """
        Validate that the name, comp_type, and strategy_type are non-empty strings.
        """
        if not isinstance(value, str) or not value.strip():
            raise ValueError("Value must be a non-empty string.")
        return value.strip()

    @field_validator("num_of_retries", mode="before")
    @classmethod
    def _validate_num_of_retries(cls, value: int) -> NonNegativeInt:
        """
        Validate that the number of retries is a non-negative integer.
        """
        if not isinstance(value, int) or value < 0:
            raise ValueError("Number of retries must be a non-negative integer.")
        return NonNegativeInt(value)

    @field_validator("file_logging", mode="before")
    @classmethod
    def _validate_file_logging(cls, value: bool) -> bool:
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

    @property
    def id(self) -> str:
        """
        Returns the unique identifier of the job.
        """
        return self._id


class JobExecution:
    """
    Runtime state for one execution of a JobDefinition.
    """

    def __init__(self, job: Job):
        self._id: str = str(uuid4())
        self._job = job
        self._max_attempts = job.num_of_retries + 1
        self._attempts: List[ExecutionAttempt] = []

    def start_attempt(self):
        if len(self._attempts) >= self._max_attempts:
            raise RuntimeError("No attempts left")
        attempt = ExecutionAttempt(len(self.attempts) + 1, self)
        self._attempts.append(attempt)

    @property
    def id(self) -> str:
        return self._id

    @property
    def job(self) -> Job:
        return self._job

    @property
    def max_attempts(self) -> int:
        return self._max_attempts

    @property
    def attempts(self) -> List["ExecutionAttempt"]:
        return self._attempts


class ExecutionAttempt:
    """
    Data for one try of a JobExecution.
    """

    def __init__(self, index: int, execution: JobExecution):
        if index < 1:
            raise ValueError("attempt number must be >= 1")
        self._id = str(uuid4())
        self._index = index
        self._error: str | None = None
        # runtime sets
        self._pending = {c.id for c in execution.job.components}
        self._succeeded = set()
        self._failed = set()
        self._cancelled = set()

    @property
    def id(self) -> str:
        return self._id

    @property
    def index(self) -> int:
        return self._index

    @property
    def error(self) -> str | None:
        return self._error

    @error.setter
    def error(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("error must be a string")
        self._error = value

    @property
    def pending(self) -> set[str]:
        """
        Components that are still pending execution.
        """
        return self._pending

    @property
    def succeeded(self) -> set[str]:
        """
        Components that have successfully executed.
        """
        return self._succeeded

    @property
    def failed(self) -> set[str]:
        """
        Components that have failed execution.
        """
        return self._failed

    @property
    def cancelled(self) -> set[str]:
        """
        Components that have been cancelled.
        """
        return self._cancelled
