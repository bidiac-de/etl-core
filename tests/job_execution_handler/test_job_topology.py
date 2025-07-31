from src.job_execution.job_execution_handler import JobExecutionHandler
from src.components.runtime_state import RuntimeState
import src.job_execution.job as job_module
from src.components.stubcomponents import StubComponent
from src.job_execution.job import Job
from datetime import datetime
from tests.helpers import get_by_temp_id

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
        "metadata": {
            "created_by": 42,
            "created_at": datetime.now(),
        },
        "component_configs": [
            {
                "id": "a",
                "name": "root",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "next": ["b", "c"],
            },
            {
                "id": "b",
                "name": "child1",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
            },
            {
                "id": "c",
                "name": "child2",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
            },
        ],
    }
    job = Job(**config)
    result = handler.execute_job(job, max_workers=2)

    exec_record = result.executions[0]
    assert exec_record.job_metrics.status == RuntimeState.SUCCESS.value
    metrics = exec_record.attempts[0].component_metrics
    expected_ids = {c.id for c in job.components.values()}
    assert set(metrics.keys()) == expected_ids

    comp1 = get_by_temp_id(job.components, job._temp_map.get("a"))
    assert metrics[comp1.id].status == RuntimeState.SUCCESS
    assert metrics[comp1.id].lines_received == 1
    comp2 = get_by_temp_id(job.components, job._temp_map.get("b"))
    assert metrics[comp2.id].status == RuntimeState.SUCCESS
    assert metrics[comp2.id].lines_received == 1
    comp3 = get_by_temp_id(job.components, job._temp_map.get("c"))
    assert metrics[comp3.id].status == RuntimeState.SUCCESS
    assert metrics[comp3.id].lines_received == 1


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
        "metadata": {
            "created_by": 42,
            "created_at": datetime.now(),
        },
        "component_configs": [
            {
                "id": "a",
                "name": "a",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "next": ["c"],
            },
            {
                "id": "b",
                "name": "b",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "next": ["c"],
            },
            {
                "id": "c",
                "name": "c",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
            },
        ],
    }
    job = Job(**config)
    result = handler.execute_job(job, max_workers=2)

    exec_record = result.executions[0]
    assert exec_record.job_metrics.status == RuntimeState.SUCCESS.value
    metrics = exec_record.attempts[0].component_metrics
    expected_ids = {c.id for c in job.components.values()}
    assert set(metrics.keys()) == expected_ids

    comp1 = get_by_temp_id(job.components, job._temp_map.get("a"))
    assert metrics[comp1.id].status == RuntimeState.SUCCESS
    assert metrics[comp1.id].lines_received == 1
    comp2 = get_by_temp_id(job.components, job._temp_map.get("b"))
    assert metrics[comp2.id].status == RuntimeState.SUCCESS
    assert metrics[comp2.id].lines_received == 1
    comp3 = get_by_temp_id(job.components, job._temp_map.get("c"))
    assert metrics[comp3.id].status == RuntimeState.SUCCESS
    assert metrics[comp3.id].lines_received == 1


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
        "metadata": {
            "created_by": 42,
            "created_at": datetime.now(),
        },
        "component_configs": [
            {
                "id": "a",
                "name": "root",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "next": ["b", "c"],
            },
            {
                "id": "b",
                "name": "a",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "next": ["d"],
            },
            {
                "id": "c",
                "name": "b",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "next": ["d"],
            },
            {
                "id": "d",
                "name": "c",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
            },
        ],
    }
    job = Job(**config)
    result = handler.execute_job(job, max_workers=2)

    exec_record = result.executions[0]
    assert exec_record.job_metrics.status == RuntimeState.SUCCESS.value
    metrics = exec_record.attempts[0].component_metrics
    expected_ids = {c.id for c in job.components.values()}
    assert set(metrics.keys()) == expected_ids

    comp1 = get_by_temp_id(job.components, job._temp_map.get("a"))
    assert metrics[comp1.id].status == RuntimeState.SUCCESS
    assert metrics[comp1.id].lines_received == 1
    comp2 = get_by_temp_id(job.components, job._temp_map.get("b"))
    assert metrics[comp2.id].status == RuntimeState.SUCCESS
    assert metrics[comp2.id].lines_received == 1
    comp3 = get_by_temp_id(job.components, job._temp_map.get("c"))
    assert metrics[comp3.id].status == RuntimeState.SUCCESS
    assert metrics[comp3.id].lines_received == 1
    comp4 = get_by_temp_id(job.components, job._temp_map.get("d"))
    assert metrics[comp4.id].status == RuntimeState.SUCCESS
    assert metrics[comp4.id].lines_received == 1
