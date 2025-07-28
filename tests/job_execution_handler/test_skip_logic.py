from src.job_execution.job_execution_handler import JobExecutionHandler
from src.job_execution.job import JobStatus
from src.components.base_component import RuntimeState
from tests.helpers import get_by_temp_id
import src.job_execution.job as job_module
from src.components.stubcomponents import StubComponent
from src.job_execution.job import Job
from datetime import datetime

# ensure Job._build_components() can find TestComponent
job_module.TestComponent = StubComponent


def test_branch_skip_fan_out(tmp_path):
    """
    Fan-out skip:
      failtest --> [child1, child2]
    comp1 should FAIL, and both child1/child2 should be SKIPPED.
    """
    handler = JobExecutionHandler()
    config = {
        "job_name": "SkipFanOutJob",
        "num_of_retries": 0,
        "file_logging": False,
        "created_by": 42,
        "created_at": datetime.now(),
        "component_configs": [
            {
                "temp_id": 1,
                "name": "root",
                "comp_type": "failtest",  # will throw
                "strategy_type": "row",
                "description": "",
                "x_coord": 0.0,
                "y_coord": 0.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
                "next": [2, 3],
            },
            {
                "temp_id": 2,
                "name": "child1",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "x_coord": 1.0,
                "y_coord": 0.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
            },
            {
                "temp_id": 3,
                "name": "child2",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "x_coord": 1.0,
                "y_coord": 1.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
            },
        ],
    }

    job = Job(**config)
    result = handler.execute_job(job, max_workers=2)

    # Job should end FAILED due to the initial failure
    exec_record = result.executions[0]
    assert exec_record.status == JobStatus.FAILED.value
    assert "One or more components failed" in exec_record.error

    # Component statuses
    comp1 = get_by_temp_id(job.components, 1)
    assert comp1.status == RuntimeState.FAILED
    for temp, expected in ((2, RuntimeState.SKIPPED), (3, RuntimeState.SKIPPED)):
        comp = get_by_temp_id(job.components, temp)
        assert comp.status == expected


def test_branch_skip_fan_in(tmp_path):
    """
    Fan-in skip:
      [ok_root, fail_root] --> join
    ok_root succeeds, fail_root fails, so join should be SKIPPED.
    """
    handler = JobExecutionHandler()
    config = {
        "job_name": "SkipFanInJob",
        "num_of_retries": 0,
        "file_logging": False,
        "created_by": 42,
        "created_at": datetime.now(),
        "component_configs": [
            {
                "temp_id": 1,
                "name": "ok_root",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "x_coord": 0.0,
                "y_coord": 0.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
                "next": [3],
            },
            {
                "temp_id": 2,
                "name": "fail_root",
                "comp_type": "failtest",
                "strategy_type": "row",
                "description": "",
                "x_coord": 0.0,
                "y_coord": 1.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
                "next": [3],
            },
            {
                "temp_id": 3,
                "name": "join",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "x_coord": 1.0,
                "y_coord": 0.5,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
            },
        ],
    }

    job = Job(**config)
    result = handler.execute_job(job, max_workers=2)

    exec_record = result.executions[0]
    assert exec_record.status == JobStatus.FAILED.value
    assert "One or more components failed" in exec_record.error

    # ok_root ran, fail_root failed, join skipped
    comp1 = get_by_temp_id(job.components, 1)
    assert comp1.status == RuntimeState.SUCCESS
    comp2 = get_by_temp_id(job.components, 2)
    assert comp2.status == RuntimeState.FAILED
    comp3 = get_by_temp_id(job.components, 3)
    assert comp3.status == RuntimeState.SKIPPED
