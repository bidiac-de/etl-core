from enum import Enum
from typing import Dict
from src.components.dataclasses import MetaData


class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class Job:
    """
    Job Objects, created by the JobExecutionHandler
    """

    def __init__(self, config: Dict):
        self.id = config.get("JobID", "default_job_id")
        self.name = config.get("JobName", "default_job_name")
        self.status = JobStatus.PENDING.value
        self.config = config
        self.metrics = []
        self.num_of_retries = config.get("NumOfRetries", 0)
        self.metadata = MetaData()
        # to-do: metadata anpassen
        self.executions = []
        self.fileLogging = "FileLogging" in config

class JobExecution:
    def __init__(self,job: Job, job_metrics, component_metrics: []):
        self.job = job
        self.job_metrics = job_metrics
        self.component_metrics = component_metrics