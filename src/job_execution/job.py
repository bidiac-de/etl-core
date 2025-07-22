from enum import Enum
from typing import Dict
from datetime import datetime
from src.components.dataclasses import MetaData


class JobStatus(Enum):
    """
    Enum representing the status of a job.
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Job:
    """
    Job Objects, created by the JobExecutionHandler
    """

    def __init__(self, config: Dict, user_id: int):
        self.id = config.get("JobID", "default_job_id")
        self.name = config.get("JobName", "default_job_name")
        self.status = JobStatus.PENDING.value
        self.config = config
        self.num_of_retries = config.get("NumOfRetries", 0)
        self.metadata = MetaData(datetime.now(), user_id)
        self.executions = []
        self.fileLogging = "FileLogging" in config


class JobExecution:
    """
    class to encapsulate the execution details of a job.
    """

    def __init__(self, job: Job, job_metrics, component_metrics: []):
        self.job = job
        self.job_metrics = job_metrics
        self.component_metrics = component_metrics
