import pytest
from src.job_execution.runtimejob import RuntimeJob
from tests.helpers import get_component_by_name
import src.job_execution.runtimejob as job_module
from src.components.stubcomponents import StubComponent
from datetime import datetime

# ensure Job._build_components() can find TestComponent
job_module.TestComponent = StubComponent


def test_create_job_with_complete_config():
    """
    A fully specified config yields matching Job fields
    """
    config = {
        "name": "TestJob",
        "num_of_retries": 3,
        "file_logging": True,
        "metadata": {
            "user_id": 42,
            "timestamp": datetime.now(),
        },
        "strategy_type": "row",
    }

    job = RuntimeJob(**config)

    assert isinstance(job, RuntimeJob)
    assert hasattr(job, "id")
    assert isinstance(job.id, str)
    assert job.name == "TestJob"
    assert job.num_of_retries == 3
    assert job.metadata_.user_id == 42
    assert job.file_logging is True


def test_create_job_with_partial_config():
    """
    Omitting keys falls back to defaults
    """
    config = {
        "metadata": {
            "user_id": 42,
            "timestamp": datetime.now(),
        }
    }

    job = RuntimeJob(**config)

    assert hasattr(job, "id")
    assert isinstance(job.id, str)
    assert job.name == "default_job_name"
    assert job.num_of_retries == 0
    assert job.file_logging is False
    assert job.strategy_type == "row"


def test_create_job_with_invalid_config_type():
    """
    Passing a non-dict as a positional argument should raise a TypeError
    """
    with pytest.raises(TypeError):
        RuntimeJob(None)


def test_create_job_with_test_component():
    config = {
        "name": "JobWithStubComponent",
        "num_of_retries": 0,
        "FileLogging": False,
        "metadata": {
            "user_id": 42,
            "timestamp": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "test1",
                "comp_type": "test",
                "description": "test dummy",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            }
        ],
    }

    job = RuntimeJob(**config)
    comp = get_component_by_name(job, "test1")
    assert comp.__class__.__name__ == "StubComponent"


def test_create_job_with_invalid_component_class():
    config = {
        "name": "InvalidComponentJob",
        "num_of_retries": 0,
        "file_logging": False,
        "metadata": {
            "user_id": 42,
            "timestamp": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "invalid1",
                "comp_type": "non_existent",
                "description": "should fail",
            }
        ],
    }

    with pytest.raises(ValueError, match="Unknown component type: 'non_existent'"):
        RuntimeJob(**config)
