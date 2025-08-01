from src.job_execution.job_execution_handler import JobExecutionHandler
from src.components.runtime_state import RuntimeState
import src.job_execution.job as job_module
from src.components.stubcomponents import StubComponent
from src.job_execution.job import Job
from tests.helpers import get_component_by_name
from datetime import datetime

# ensure Job._build_components() can find TestComponent
job_module.TestComponent = StubComponent


def test_branch_skip_fan_out(tmp_path):
    """
    Fan-out cancellation:
      failtest --> [child1, child2]
    comp1 should FAIL, and both child1/child2 should be CANCELLED.
    """
    handler = JobExecutionHandler()
    config = {
        "job_name": "SkipFanOutJob",
        "num_of_retries": 0,
        "file_logging": False,
        "metadata": {
            "created_by": 42,
            "created_at": datetime.now(),
        },
        "components": [
            {
                "name": "root",
                "comp_type": "failtest",  # will throw
                "strategy_type": "row",
                "description": "",
                "next": ["child1", "child2"],
            },
            {
                "name": "child1",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
            },
            {
                "name": "child2",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
            },
        ],
    }

    job = Job(**config)
    result = handler.execute_job(job, max_workers=2)

    # Job should end FAILED due to the initial failure
    exec_record = result.executions[0]
    metrics = exec_record.attempts[0].component_metrics
    assert exec_record.job_metrics.status == RuntimeState.FAILED.value
    assert "One or more components failed" in exec_record.attempts[0].error

    # Component statuses
    comp1 = get_component_by_name(job, "root")
    assert metrics[comp1.id].status == RuntimeState.FAILED
    comp2 = get_component_by_name(job, "child1")
    assert metrics[comp2.id].status == RuntimeState.CANCELLED
    comp3 = get_component_by_name(job, "child2")
    assert metrics[comp3.id].status == RuntimeState.CANCELLED


def test_branch_skip_fan_in(tmp_path):
    """
    Fan-in cancellation:
      [ok_root, fail_root] --> join
    ok_root succeeds, fail_root fails, so join should be CANCELLED.
    """
    handler = JobExecutionHandler()
    config = {
        "job_name": "SkipFanInJob",
        "num_of_retries": 0,
        "file_logging": False,
        "metadata": {
            "created_by": 42,
            "created_at": datetime.now(),
        },
        "components": [
            {
                "name": "ok_root",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "next": ["join"],
            },
            {
                "name": "fail_root",
                "comp_type": "failtest",
                "strategy_type": "row",
                "description": "",
                "next": ["join"],
            },
            {
                "name": "join",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
            },
        ],
    }

    job = Job(**config)
    result = handler.execute_job(job, max_workers=2)

    exec_record = result.executions[0]
    metrics = exec_record.attempts[0].component_metrics
    assert exec_record.job_metrics.status == RuntimeState.FAILED.value
    assert "One or more components failed" in exec_record.attempts[0].error

    # ok_root ran, fail_root failed, join skipped
    comp1 = get_component_by_name(job, "ok_root")
    assert metrics[comp1.id].status == RuntimeState.SUCCESS
    comp2 = get_component_by_name(job, "fail_root")
    assert metrics[comp2.id].status == RuntimeState.FAILED
    comp3 = get_component_by_name(job, "join")
    assert metrics[comp3.id].status == RuntimeState.CANCELLED


def test_chain_skip_linear():
    """
    Chain cancellation:
      root --> middle --> leaf
    comp1 should FAIL, and both middle/leaf should be CANCELLED.
    """
    handler = JobExecutionHandler()
    config = {
        "job_name": "ChainSkipJob",
        "num_of_retries": 0,
        "file_logging": False,
        "metadata": {
            "created_by": 42,
            "created_at": datetime.now(),
        },
        "components": [
            {
                "name": "root",
                "comp_type": "failtest",
                "strategy_type": "row",
                "description": "",
                "next": ["middle"],
            },
            {
                "name": "middle",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "next": ["leaf"],
            },
            {
                "name": "leaf",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
            },
        ],
    }

    job = Job(**config)
    result = handler.execute_job(job, max_workers=1)

    exec_record = result.executions[0]
    metrics = exec_record.attempts[0].component_metrics
    assert exec_record.job_metrics.status == RuntimeState.FAILED.value
    assert "One or more components failed" in exec_record.attempts[0].error

    comp1 = get_component_by_name(job, "root")
    comp2 = get_component_by_name(job, "middle")
    comp3 = get_component_by_name(job, "leaf")
    assert metrics[comp1.id].status == RuntimeState.FAILED
    assert metrics[comp2.id].status == RuntimeState.CANCELLED
    assert metrics[comp3.id].status == RuntimeState.CANCELLED


def test_skip_diamond():
    """
    cancellation due to cancelled predecessor:
      a --> b / c -->d
    d should be CANCELLED since one predecessor was cancelled.
    """
    handler = JobExecutionHandler()
    config = {
        "job_name": "SkipDiamondJob",
        "num_of_retries": 0,
        "file_logging": False,
        "metadata": {
            "created_by": 42,
            "created_at": datetime.now(),
        },
        "components": [
            {
                "name": "a",
                "comp_type": "failtest",
                "strategy_type": "row",
                "description": "",
                "next": ["b", "c"],
            },
            {
                "name": "b",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "next": ["d"],
            },
            {
                "name": "c",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "next": ["d"],
            },
            {
                "name": "d",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
            },
        ],
    }

    job = Job(**config)
    result = handler.execute_job(job, max_workers=2)

    exec_record = result.executions[0]
    metrics = exec_record.attempts[0].component_metrics
    assert exec_record.job_metrics.status == RuntimeState.FAILED.value
    assert "One or more components failed" in exec_record.attempts[0].error

    comp1 = get_component_by_name(job, "a")
    comp2 = get_component_by_name(job, "b")
    comp3 = get_component_by_name(job, "c")
    comp4 = get_component_by_name(job, "d")
    assert metrics[comp1.id].status == RuntimeState.FAILED
    assert metrics[comp2.id].status == RuntimeState.CANCELLED
    assert metrics[comp3.id].status == RuntimeState.CANCELLED
    assert metrics[comp4.id].status == RuntimeState.CANCELLED
