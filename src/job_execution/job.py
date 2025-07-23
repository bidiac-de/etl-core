from enum import Enum
from typing import Dict, Any, List, Optional
from datetime import datetime
from src.components.dataclasses import MetaData
from pydantic import BaseModel, Field, ConfigDict
from src.components.base_component import Component
from src.components.registry import component_registry


class JobStatus(Enum):
    """
    Enum representing the status of a job.
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Job(BaseModel):
    """
    Job Objects, created by the JobExecutionHandler
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

    id: str = Field(default_factory=lambda: "default_job_id")
    name: str = Field(default_factory=lambda: "default_job_name")
    status: str = Field(default="pending")
    config: Dict[str, Any]
    num_of_retries: int = Field(default=0)
    metadata: MetaData = Field(default=None, exclude=True)
    executions: List[Any] = Field(default_factory=list)
    file_logging: bool = Field(default=False)
    components: Dict[str, Component] = Field(default_factory=dict, exclude=True)
    started_at: datetime = Field(default_factory=datetime.now, exclude=True)
    completed_at: datetime = Field(default=None, exclude=True)
    error: Optional[str] = Field(default=None, exclude=True)

    def __init__(self, config: Dict, user_id: int):
        super().__init__(
            id=config.get("JobID", "default_job_id"),
            name=config.get("JobName", "default_job_name"),
            status=JobStatus.PENDING.value,
            config=config,
            num_of_retries=config.get("NumOfRetries", 0),
            metadata=MetaData(datetime.now(), user_id),
            file_logging=config.get("FileLogging", False),
        )
        self.metadata = MetaData(datetime.now(), user_id)
        self._build_components()

    def _build_components(self) -> None:
        """
        Instantiate and connect every component named in config
        """
        comps: Dict[str, Component] = {}

        # build components
        for cconf in self.config.get("components", []):
            comp_name = cconf["name"]
            comp_type = cconf["comp_type"]
            if comp_type not in self.component_registry:
                raise ValueError(f"Unknown component type: {comp_type}")

            component_class = component_registry[comp_type]
            component = component_class(**cconf)

            comps[comp_name] = component

        # connect components
        for cconf in self.config.get("components", []):
            src = comps[cconf["name"]]
            for nxt_name in cconf.get("next", []):
                if nxt_name not in comps:
                    raise ValueError(f"Unknown next‐component: {nxt_name}")
                src.add_next(comps[nxt_name])

        self.components = comps


class JobExecution:
    """
    class to encapsulate the execution details of a job.
    """

    def __init__(self, job: Job, job_metrics, component_metrics: []):
        self.job = job
        self.job_metrics = job_metrics
        self.component_metrics = component_metrics
