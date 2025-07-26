from src.job_execution.job_execution_handler import JobExecutionHandler
from src.job_execution.job import JobStatus
from src.components.base_component import RuntimeState


def test_branch_skip_fan_out(tmp_path):
    """
    Fan-out skip:
      failtest --> [child1, child2]
    comp1 should FAIL, and both child1/child2 should be SKIPPED.
    """
    handler = JobExecutionHandler()
    config = {
        "JobID": "skip_fan_out",
        "JobName": "SkipFanOutJob",
        "NumOfRetries": 0,
        "FileLogging": False,
        "components": [
            {
                "id": 1,
                "name": "root",
                "comp_type": "failtest",  # will throw
                "description": "",
                "x_coord": 0.0,
                "y_coord": 0.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
                "next": ["child1", "child2"],
            },
            {
                "id": 2,
                "name": "child1",
                "comp_type": "test",
                "description": "",
                "x_coord": 1.0,
                "y_coord": 0.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
            },
            {
                "id": 3,
                "name": "child2",
                "comp_type": "test",
                "description": "",
                "x_coord": 1.0,
                "y_coord": 1.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
            },
        ],
    }

    job = handler.create_job(config, user_id=1)
    result = handler.execute_job(job, max_workers=2)

    # Job should end FAILED due to the initial failure
    exec_record = result.executions[0]
    assert exec_record.status == JobStatus.FAILED.value
    assert "One or more components failed" in exec_record.error

    # Component statuses
    assert job.components["root"].status == RuntimeState.FAILED
    assert job.components["child1"].status == RuntimeState.SKIPPED
    assert job.components["child2"].status == RuntimeState.SKIPPED


def test_branch_skip_fan_in(tmp_path):
    """
    Fan-in skip:
      [ok_root, fail_root] --> join
    ok_root succeeds, fail_root fails, so join should be SKIPPED.
    """
    handler = JobExecutionHandler()
    config = {
        "JobID": "skip_fan_in",
        "JobName": "SkipFanInJob",
        "NumOfRetries": 0,
        "FileLogging": False,
        "components": [
            {
                "id": 1,
                "name": "ok_root",
                "comp_type": "test",
                "description": "",
                "x_coord": 0.0,
                "y_coord": 0.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
                "next": ["join"],
            },
            {
                "id": 2,
                "name": "fail_root",
                "comp_type": "failtest",
                "description": "",
                "x_coord": 0.0,
                "y_coord": 1.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
                "next": ["join"],
            },
            {
                "id": 3,
                "name": "join",
                "comp_type": "test",
                "description": "",
                "x_coord": 1.0,
                "y_coord": 0.5,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
            },
        ],
    }

    job = handler.create_job(config, user_id=1)
    result = handler.execute_job(job, max_workers=2)

    exec_record = result.executions[0]
    assert exec_record.status == JobStatus.FAILED.value
    assert "One or more components failed" in exec_record.error

    # ok_root ran, fail_root failed, join skipped
    assert job.components["ok_root"].status == RuntimeState.SUCCESS
    assert job.components["fail_root"].status == RuntimeState.FAILED
    assert job.components["join"].status == RuntimeState.SKIPPED
