from typing import Dict, Any, List
from datetime import datetime
from src.components.dataclasses import MetaData
from pydantic import BaseModel, Field, ConfigDict, NonNegativeInt, model_validator
from src.components.base_component import Component
from src.components.registry import component_registry
from src.metrics.base_metrics import Metrics
from src.metrics.job_metrics import JobMetrics
from src.components.base_component import RuntimeState
from uuid import uuid4


class Job(BaseModel):
    """
    Job Objects, created by the JobExecutionHandler
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

    id: str = Field(default_factory=lambda: str(uuid4()), exclude=True)
    name: str = Field(default="default_job_name")
    num_of_retries: NonNegativeInt = Field(default=0)
    file_logging: bool = Field(default=False)
    created_by: int
    created_at: datetime = Field(default_factory=datetime.now)
    component_configs: List[Dict[str, Any]] = Field(default_factory=list)

    config: Dict[str, Any] = Field(default_factory=dict, exclude=True)

    # runtime-objects
    metadata: MetaData = Field(default=None, exclude=True)
    executions: List[Any] = Field(default_factory=list, exclude=True)

    components: Dict[str, Component] = Field(default_factory=dict, exclude=True)

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
        After Pydantic has set all fields, build and connect components.
        """
        # build and connect just as before
        self.metadata = MetaData(
            created_by=self.created_by,
            created_at=self.created_at,
        )
        self._build_components()
        self._connect_components()
        return self

    def _build_components(self) -> None:
        """
        Instantiate every component named in config
        """
        comps: Dict[str, Component] = {}

        # map of temporary IDs to internal ids
        self._temp_map: Dict[int, str] = {}

        for cconf in self.config.get("component_configs", []):
            comp_type = cconf["comp_type"]
            if comp_type not in component_registry:
                raise ValueError(f"Unknown component type: {comp_type}")

            component_class = component_registry[comp_type]
            component = component_class(**cconf)

            comps[component.id] = component
            self._temp_map[component.temp_id] = component.id

        self.components = comps

    def _connect_components(self) -> None:
        """
        Connect components based on the configuration
        """
        for cconf in self.config.get("component_configs", []):
            src_uuid = self._temp_map.get(cconf["temp_id"])
            src = self.components[src_uuid]
            for nxt_temp_id in cconf.get("next", []):
                dest_uuid = self._temp_map.get(nxt_temp_id)
                if dest_uuid not in self.components:
                    raise ValueError(f"Unknown next-component: {nxt_temp_id}")

                dest = self.components[dest_uuid]
                src.add_next(dest)
                dest.add_prev(src)


class JobExecution:
    """
    class to encapsulate the execution details of a job
    """

    job: Job
    job_metrics: JobMetrics = None
    status: str = RuntimeState.PENDING.value
    error: str = None
    component_metrics: Dict[str, Metrics]

    def __init__(self, job: Job, status: str = RuntimeState.PENDING.value):
        self.job = job
        self.status = status
