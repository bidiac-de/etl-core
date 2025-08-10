from typing import List, Dict
from src.components.dataclasses import MetaData
from pydantic import (
    Field,
    ConfigDict,
    model_validator,
    PrivateAttr,
)
import asyncio

from src.components.base_component import Component, get_strategy
from src.job_execution.retry_strategy import RetryStrategy, ConstantRetryStrategy
from src.persistance.base_models.job_base import JobBase
from uuid import uuid4
import logging

logger = logging.getLogger("job.ExecutionHandler")


class _Sentinel:
    """Unique end-of-stream marker for each component."""

    __slots__ = ("component_id",)

    def __init__(self, component_id: str) -> None:
        self.component_id = component_id

    def __repr__(self) -> str:
        return f"<Sentinel {self.component_id}>"


class RuntimeJob(JobBase):
    """
    Job Objects
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True, extra="ignore", validate_assignment=True
    )
    components: List[Component] = Field(default_factory=list)
    metadata_: MetaData = Field(default_factory=lambda: MetaData(), alias="metadata")
    _id: str = PrivateAttr(default_factory=lambda: str(uuid4()))

    @model_validator(mode="after")
    def _assign_strategies(self) -> "JobBase":
        """
        After wiring, give every component the Job’s strategy.
        """
        for comp in self.components:
            # override whatever was on the component; use job-level strategy_type
            comp.strategy = get_strategy(self.strategy_type)

        return self

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

    def __init__(self, job: RuntimeJob):
        self._id: str = str(uuid4())
        self._job = job
        # each execution carries its own retry strategy
        self._retry_strategy = ConstantRetryStrategy(job.num_of_retries)
        self._max_attempts = job.num_of_retries + 1
        self._attempts: List[ExecutionAttempt] = []

        # each component gets its own sentinel instance
        self._sentinels: Dict[str, _Sentinel] = {
            comp.id: _Sentinel(comp.id) for comp in job.components
        }

    def start_attempt(self):
        if len(self._attempts) >= self._max_attempts:
            raise RuntimeError("No attempts left")
        attempt = ExecutionAttempt(len(self.attempts) + 1, self)
        self._attempts.append(attempt)

    def latest_attempt(self) -> "ExecutionAttempt":
        if not self._attempts:
            raise RuntimeError("No attempts have been started yet")
        return self._attempts[-1]

    @property
    def id(self) -> str:
        return self._id

    @property
    def job(self) -> RuntimeJob:
        return self._job

    @property
    def max_attempts(self) -> int:
        return self._max_attempts

    @property
    def attempts(self) -> List["ExecutionAttempt"]:
        return self._attempts

    @property
    def retry_strategy(self) -> RetryStrategy:
        """
        Returns the retry strategy for this job execution.
        """
        return self._retry_strategy

    @property
    def sentinels(self) -> Dict[str, _Sentinel]:
        """
        Returns a mapping of component IDs to their sentinels.
        Sentinels are used to mark the end of a stream for each component.
        """
        return self._sentinels


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
        self._current_tasks: Dict[str, asyncio.Task] = {}

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

    @property
    def current_tasks(self) -> Dict[str, asyncio.Task]:
        """
        Returns a mapping of component IDs to their current asyncio tasks.
        This is used to track the execution of components in the job.
        """
        return self._current_tasks

    @current_tasks.setter
    def current_tasks(self, tasks: Dict[str, asyncio.Task]) -> None:
        """
        Sets the current tasks for the job execution.
        This is used to track the execution of components in the job.
        """
        if not isinstance(tasks, dict):
            raise TypeError(
                "current_tasks must be a dictionary of component IDs to asyncio tasks"
            )
        self._current_tasks = tasks
