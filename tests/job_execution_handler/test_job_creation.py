import pytest
from src.job_execution.job_execution_handler import JobExecutionHandler
from src.job_execution.job import Job


def test_create_job_with_complete_config():
    """
    A fully specified config yields matching Job fields
    """
    handler = JobExecutionHandler()
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
    assert job.num_of_retries == 3
    assert job.metadata.created_by == user_id
    assert job.executions == []
    assert job.file_logging is True


def test_create_job_with_partial_config():
    """
    Omitting keys falls back to defaults
    """
    handler = JobExecutionHandler()
    config = {}
    user_id = 7

    job = handler.create_job(config, user_id)

    assert job.id == "default_job_id"
    assert job.name == "default_job_name"
    assert job.num_of_retries == 0
    assert job.executions == []
    assert job.file_logging is False


def test_create_job_with_invalid_config_type():
    """
    Passing a non-dict config should raise an AttributeError
    """
    handler = JobExecutionHandler()
    with pytest.raises(AttributeError):
        handler.create_job(None, 1)


def test_create_job_with_test_component():
    handler = JobExecutionHandler()
    config = {
        "JobID": "test_1",
        "JobName": "JobWithStubComponent",
        "NumOfRetries": 0,
        "FileLogging": False,
        "components": [
            {
                "id": 1,
                "name": "test1",
                "comp_type": "test",
                "description": "test dummy",
                "x_coord": 0.0,
                "y_coord": 0.0,
                "created_by": 42,
                "created_at": "2024-01-01T00:00:00",
            }
        ],
    }

    job = handler.create_job(config, user_id=42)
    assert "test1" in job.components
    assert job.components["test1"].__class__.__name__ == "StubComponent"
    assert job.components["test1"].status == "PENDING"


def test_create_job_with_invalid_component_class():
    handler = JobExecutionHandler()
    config = {
        "JobID": "fail_1",
        "JobName": "InvalidComponentJob",
        "NumOfRetries": 0,
        "FileLogging": False,
        "components": [
            {
                "id": 1,
                "name": "invalid1",
                "comp_type": "non_existent",
                "description": "should fail",
                "x_coord": 0.0,
                "y_coord": 0.0,
                "created_by": 1,
                "created_at": "2024-01-01T00:00:00",
            }
        ],
    }

    with pytest.raises(ValueError, match="Unknown component type: non_existent"):
        handler.create_job(config, user_id=1)
