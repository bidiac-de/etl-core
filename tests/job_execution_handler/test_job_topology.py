from src.job_execution.job_execution_handler import JobExecutionHandler
from src.job_execution.job import JobStatus
from src.components.base_component import RuntimeState

def test_fan_out_topology():
    """
    root --> [child1, child2]
    All three should run to SUCCESS, and record metrics.lines_received == 1
    """
    handler = JobExecutionHandler()
    config = {
        "JobID": "fan_out",
        "JobName": "FanOutJob",
        "NumOfRetries": 0,
        "FileLogging": False,
        "components": [
            {
                "id": 1,
                "name": "root",
                "comp_type": "test",
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

    exec_record = result.executions[0]
    assert exec_record.status == JobStatus.COMPLETED.value
    metrics = exec_record.component_metrics
    assert set(metrics.keys()) == {"root", "child1", "child2"}

    for name in metrics:
        assert job.components[name].status == RuntimeState.SUCCESS
        assert metrics[name].lines_received == 1

def test_fan_in_topology():
    """
    [a, b] --> c
    a and b run in parallel, then c runs after both complete
    """
    handler = JobExecutionHandler()
    config = {
        "JobID": "fan_in",
        "JobName": "FanInJob",
        "NumOfRetries": 0,
        "FileLogging": False,
        "components": [
            {
                "id": 1,
                "name": "a",
                "comp_type": "test",
                "description": "",
                "x_coord": 0.0,
                "y_coord": 0.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
                "next": ["c"],
            },
            {
                "id": 2,
                "name": "b",
                "comp_type": "test",
                "description": "",
                "x_coord": 0.0,
                "y_coord": 1.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
                "next": ["c"],
            },
            {
                "id": 3,
                "name": "c",
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
    assert exec_record.status == JobStatus.COMPLETED.value
    metrics = exec_record.component_metrics
    assert set(metrics.keys()) == {"a", "b", "c"}

    for name in metrics:
        assert job.components[name].status == RuntimeState.SUCCESS
        assert metrics[name].lines_received == 1

def test_diamond_topology():
    """
    root --> [a, b] --> c
    A classic diamond: root fans out to a,b then joins at c
    """
    handler = JobExecutionHandler()
    config = {
        "JobID": "diamond",
        "JobName": "DiamondJob",
        "NumOfRetries": 0,
        "FileLogging": False,
        "components": [
            {
                "id": 1,
                "name": "root",
                "comp_type": "test",
                "description": "",
                "x_coord": 0.0,
                "y_coord": 0.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
                "next": ["a", "b"],
            },
            {
                "id": 2,
                "name": "a",
                "comp_type": "test",
                "description": "",
                "x_coord": 1.0,
                "y_coord": 0.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
                "next": ["c"],
            },
            {
                "id": 3,
                "name": "b",
                "comp_type": "test",
                "description": "",
                "x_coord": 1.0,
                "y_coord": 1.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
                "next": ["c"],
            },
            {
                "id": 4,
                "name": "c",
                "comp_type": "test",
                "description": "",
                "x_coord": 2.0,
                "y_coord": 0.5,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
            },
        ],
    }
    job = handler.create_job(config, user_id=1)
    result = handler.execute_job(job, max_workers=2)

    exec_record = result.executions[0]
    assert exec_record.status == JobStatus.COMPLETED.value
    metrics = exec_record.component_metrics
    assert set(metrics.keys()) == {"root", "a", "b", "c"}

    for name in metrics:
        assert job.components[name].status == RuntimeState.SUCCESS
        assert metrics[name].lines_received == 1
