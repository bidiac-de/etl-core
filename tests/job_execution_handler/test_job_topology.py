from src.job_execution.job_execution_handler import JobExecutionHandler
from src.components.runtime_state import RuntimeState
import src.job_execution.job as job_module
from src.components.stubcomponents import StubComponent
from src.job_execution.job import Job
from tests.helpers import get_component_by_name
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
        "metadata": {
            "created_by": 42,
            "created_at": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "root",
                "comp_type": "test",
                "description": "",
                "next": ["child1", "child2"],
            },
            {
                "name": "child1",
                "comp_type": "test",
                "description": "",
            },
            {
                "name": "child2",
                "comp_type": "test",
                "description": "",
            },
        ],
    }
    job = Job(**config)
    execution = handler.execute_job(job)
    attempt = execution.attempts[0]
    assert len(execution.attempts) == 1
    mh = handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    comp1 = get_component_by_name(job, "root")
    comp2 = get_component_by_name(job, "child1")
    comp3 = get_component_by_name(job, "child2")

    comp1_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp1.id)
    comp2_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp2.id)
    comp3_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp3.id)

    assert comp1_metrics.lines_received == 1
    assert comp1_metrics.status == RuntimeState.SUCCESS
    assert comp2_metrics.lines_received == 1
    assert comp2_metrics.status == RuntimeState.SUCCESS
    assert comp3_metrics.lines_received == 1
    assert comp3_metrics.status == RuntimeState.SUCCESS


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
        "strategy_type": "row",
        "components": [
            {
                "name": "a",
                "comp_type": "test",
                "description": "",
                "next": ["c"],
            },
            {
                "name": "b",
                "comp_type": "test",
                "description": "",
                "next": ["c"],
            },
            {
                "name": "c",
                "comp_type": "test",
                "description": "",
            },
        ],
    }
    job = Job(**config)
    execution = handler.execute_job(job)
    attempt = execution.attempts[0]
    assert len(execution.attempts) == 1
    mh = handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    comp1 = get_component_by_name(job, "a")
    comp2 = get_component_by_name(job, "b")
    comp3 = get_component_by_name(job, "c")

    comp1_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp1.id)
    comp2_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp2.id)
    comp3_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp3.id)

    assert comp1_metrics.lines_received == 1
    assert comp1_metrics.status == RuntimeState.SUCCESS
    assert comp2_metrics.lines_received == 1
    assert comp2_metrics.status == RuntimeState.SUCCESS
    assert comp3_metrics.lines_received == 2  # both a and b send one row to c
    assert comp3_metrics.status == RuntimeState.SUCCESS


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
        "strategy_type": "row",
        "components": [
            {
                "name": "root",
                "comp_type": "test",
                "description": "",
                "next": ["a", "b"],
            },
            {
                "name": "a",
                "comp_type": "test",
                "description": "",
                "next": ["c"],
            },
            {
                "name": "b",
                "comp_type": "test",
                "description": "",
                "next": ["c"],
            },
            {
                "name": "c",
                "comp_type": "test",
                "description": "",
            },
        ],
    }
    job = Job(**config)
    execution = handler.execute_job(job)
    attempt = execution.attempts[0]
    assert len(execution.attempts) == 1
    mh = handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    comp1 = get_component_by_name(job, "root")
    comp2 = get_component_by_name(job, "a")
    comp3 = get_component_by_name(job, "b")
    comp4 = get_component_by_name(job, "c")

    comp1_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp1.id)
    comp2_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp2.id)
    comp3_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp3.id)
    comp4_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp4.id)

    assert comp1_metrics.lines_received == 1
    assert comp1_metrics.status == RuntimeState.SUCCESS
    assert comp2_metrics.lines_received == 1
    assert comp2_metrics.status == RuntimeState.SUCCESS
    assert comp3_metrics.lines_received == 1
    assert comp3_metrics.status == RuntimeState.SUCCESS
    assert comp4_metrics.lines_received == 2
    assert comp4_metrics.status == RuntimeState.SUCCESS
