from typing import Dict, Any, List
from datetime import datetime
from src.components.dataclasses import MetaData
from pydantic import BaseModel, Field, ConfigDict, NonNegativeInt
from src.components.base_component import Component
from src.components.registry import component_registry
from src.metrics.base_metrics import Metrics
from src.metrics.job_metrics import JobMetrics
from src.job_execution.job_status import JobStatus


class Job(BaseModel):
    """
    Job Objects, created by the JobExecutionHandler
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

    id: str = Field(default_factory=lambda: "default_job_id")
    name: str = Field(default_factory=lambda: "default_job_name")
    config: Dict[str, Any]
    num_of_retries: NonNegativeInt = Field(default=0)
    metadata: MetaData = Field(default=None, exclude=True)
    executions: List[Any] = Field(default_factory=list, exclude=True)
    file_logging: bool = Field(default=False)
    components: Dict[str, Component] = Field(default_factory=dict, exclude=True)

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
        self._build_components()
        self._connect_components()

    def _build_components(self) -> None:
        """
        Instantiate every component named in config
        """
        comps: Dict[str, Component] = {}

        # build components
        for cconf in self.config.get("components", []):
            comp_name = cconf["name"]
            comp_type = cconf["comp_type"]
            if comp_type not in component_registry:
                raise ValueError(f"Unknown component type: {comp_type}")

            component_class = component_registry[comp_type]
            component = component_class(**cconf)

            comps[comp_name] = component

        self.components = comps

    def _connect_components(self) -> None:
        """
        Connect components based on the configuration
        """
        for cconf in self.config.get("components", []):
            src = self.components[cconf["name"]]
            for nxt_name in cconf.get("next", []):
                if nxt_name not in self.components:
                    raise ValueError(f"Unknown next-component: {nxt_name}")
                src.add_next(self.components[nxt_name])
                self.components[nxt_name].add_prev(src)


class JobExecution:
    """
    class to encapsulate the execution details of a job
    """

    job: Job
    completed_at: datetime = None
    job_metrics: JobMetrics = None
    status: str = JobStatus.PENDING.value
    error: str = None
    component_metrics: Dict[str, Metrics]

    def __init__(self, job: Job, status: str = JobStatus.PENDING.value):
        self.job = job
        self.status = status
