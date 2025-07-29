from typing import Dict, Any, List
from datetime import datetime
from src.components.dataclasses import MetaData
from pydantic import BaseModel, Field, ConfigDict, NonNegativeInt, model_validator
from src.components.base_component import Component
from src.components.component_registry import component_registry
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.metrics.job_metrics import JobMetrics
from src.components.runtime_state import RuntimeState
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


class JobExecution:
    """
    class to encapsulate the execution details of a job
    """

    job: Job
    job_metrics: JobMetrics = None
    status: str = RuntimeState.PENDING.value
    error: str = None
    component_metrics: Dict[str, ComponentMetrics]

    def __init__(self, job: Job, status: str = RuntimeState.PENDING.value):
        self.job = job
        self.status = status
