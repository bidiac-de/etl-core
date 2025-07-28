from src.job_execution.job_execution_handler import JobExecutionHandler
from src.job_execution.job import JobStatus
from src.components.base_component import RuntimeState
import src.job_execution.job as job_module
from src.components.stubcomponents import StubComponent
from src.job_execution.job import Job
from datetime import datetime

# ensure Job._build_components() can find TestComponent
job_module.TestComponent = StubComponent


def test_fan_out_topology():
    """
    root --> [child1, child2]
    All three should run to SUCCESS, and record metrics.lines_received == 1
    """
    handler = JobExecutionHandler()
    config = {
        "job_name": "FanOutJob",
        "num_of_retries": 0,
        "file_logging": False,
        "created_by": 42,
        "created_at": datetime.now(),
        "component_configs": [
            {
                "temp_id": 1,
                "name": "root",
                "comp_type": "test",
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

    exec_record = result.executions[0]
    assert exec_record.status == JobStatus.COMPLETED.value
    metrics = exec_record.component_metrics
    expected_ids = {c.id for c in job.components.values()}
    assert set(metrics.keys()) == expected_ids

    for uuid in metrics:
        comp = job.components[uuid]
        assert comp.status == RuntimeState.SUCCESS
        assert metrics[uuid].lines_received == 1


def test_fan_in_topology():
    """
    [a, b] --> c
    a and b run in parallel, then c runs after both complete
    """
    handler = JobExecutionHandler()
    config = {
        "job_name": "FanInJob",
        "num_of_retries": 0,
        "file_logging": False,
        "created_by": 42,
        "created_at": datetime.now(),
        "component_configs": [
            {
                "temp_id": 1,
                "name": "a",
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
                "name": "b",
                "comp_type": "test",
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
                "name": "c",
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
    assert exec_record.status == JobStatus.COMPLETED.value
    metrics = exec_record.component_metrics
    expected_ids = {c.id for c in job.components.values()}
    assert set(metrics.keys()) == expected_ids

    for uuid in metrics:
        comp = job.components[uuid]
        assert comp.status == RuntimeState.SUCCESS
        assert metrics[uuid].lines_received == 1


def test_diamond_topology():
    """
    root --> [a, b] --> c
    A classic diamond: root fans out to a,b then joins at c
    """
    handler = JobExecutionHandler()
    config = {
        "job_name": "DiamondJob",
        "num_of_retries": 0,
        "file_logging": False,
        "created_by": 42,
        "created_at": datetime.now(),
        "component_configs": [
            {
                "temp_id": 1,
                "name": "root",
                "comp_type": "test",
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
                "name": "a",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "x_coord": 1.0,
                "y_coord": 0.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
                "next": [4],
            },
            {
                "temp_id": 3,
                "name": "b",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "x_coord": 1.0,
                "y_coord": 1.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
                "next": [4],
            },
            {
                "temp_id": 4,
                "name": "c",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "x_coord": 2.0,
                "y_coord": 0.5,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
            },
        ],
    }
    job = Job(**config)
    result = handler.execute_job(job, max_workers=2)

    exec_record = result.executions[0]
    assert exec_record.status == JobStatus.COMPLETED.value
    metrics = exec_record.component_metrics
    expected_ids = {c.id for c in job.components.values()}
    assert set(metrics.keys()) == expected_ids

    for uuid in metrics:
        comp = job.components[uuid]
        assert comp.status == RuntimeState.SUCCESS
        assert metrics[uuid].lines_received == 1
