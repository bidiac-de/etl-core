import pytest
from src.job_execution.job_execution_handler import JobExecutionHandler
from src.job_execution.job import Job, JobStatus


def test_create_job_with_complete_config():
    """
    A fully specified config yields matching Job fields
    """
    handler = JobExecutionHandler(component_registry={})
    config = {
        "JobID": "123",
        "JobName": "TestJob",
        "NumOfRetries": 3,
        "FileLogging": True,
    }
    user_id = 42

    job = handler.create_job(config, user_id)

    assert isinstance(job, Job)
    assert job.id == "123"
    assert job.name == "TestJob"
    assert job.status == JobStatus.PENDING.value
    assert job.num_of_retries == 3
    assert job.metadata.created_by == user_id
    assert job.executions == []
    assert job.fileLogging is True


def test_create_job_with_partial_config():
    """
    Omitting keys falls back to defaults
    """
    handler = JobExecutionHandler(component_registry={})
    config = {}
    user_id = 7

    job = handler.create_job(config, user_id)

    assert job.id == "default_job_id"
    assert job.name == "default_job_name"
    assert job.status == JobStatus.PENDING.value
    assert job.num_of_retries == 0
    assert job.executions == []
    assert job.fileLogging is False


def test_create_job_with_invalid_config_type():
    """
    Passing a non‐dict config should raise an AttributeError
    """
    handler = JobExecutionHandler(component_registry={})
    with pytest.raises(AttributeError):
        handler.create_job(None, 1)
