import pytest
from src.job_execution.job import Job
from tests.helpers import get_component_by_name
import src.job_execution.job as job_module
from src.components.stubcomponents import StubComponent
from datetime import datetime

# ensure Job can resolve "test" to the stub component in registry
job_module.TestComponent = StubComponent


def _simple_schema():
    # minimal single-field schema, used for all tests
    return {"fields": [{"name": "id", "data_type": "integer", "nullable": False}]}


def test_create_job_with_complete_config():
    """
    A fully specified config yields matching Job fields
    """
    config = {
        "name": "TestJob",
        "num_of_retries": 3,
        "file_logging": True,
        "metadata": {
            "created_by": 42,
            "created_at": datetime.now(),
        },
        "strategy_type": "row",
    }

    job = Job(**config)

    assert isinstance(job, Job)
    assert hasattr(job, "id")
    assert isinstance(job.id, str)
    assert job.name == "TestJob"
    assert job.num_of_retries == 3
    assert job.metadata.created_by == 42
    assert job.file_logging is True


def test_create_job_with_partial_config():
    """
    Omitting keys falls back to defaults
    """
    config = {
        "created_by": 42,
    }

    job = Job(**config)

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
        Job(None)  # type: ignore[arg-type]


def test_create_job_with_test_component():
    config = {
        "name": "JobWithStubComponent",
        "num_of_retries": 0,
        "FileLogging": False,
        "metadata": {
            "created_by": 42,
            "created_at": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "test1",
                "comp_type": "test",
                "description": "test dummy",
                "routes": {"out": []},
                "in_port_schemas": {"in": _simple_schema()},
                "port_schemas": {"out": _simple_schema()},
            }
        ],
    }

    job = Job(**config)
    comp = get_component_by_name(job, "test1")
    assert comp.__class__.__name__ == "StubComponent"


def test_create_job_with_invalid_component_class():
    config = {
        "name": "InvalidComponentJob",
        "num_of_retries": 0,
        "file_logging": False,
        "metadata": {
            "created_by": 42,
            "created_at": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "invalid1",
                "comp_type": "non_existent",
                "strategy_type": "row",
                "description": "should fail",
            }
        ],
    }

    with pytest.raises(ValueError, match="Unknown component type: 'non_existent'"):
        Job(**config)
