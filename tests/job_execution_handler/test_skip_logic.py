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
        "name": "SkipFanOutJob",
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
                "comp_type": "failtest",  # will throw
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

    # Job-level assertions
    assert mh.get_job_metrics(execution.id).status == RuntimeState.FAILED
    assert attempt.error is not None
    assert "fail stubcomponent failed" in attempt.error

    # Component statuses
    comp1 = get_component_by_name(job, "root")
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, comp1.id).status
        == RuntimeState.FAILED
    )
    comp2 = get_component_by_name(job, "child1")
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, comp2.id).status
        == RuntimeState.CANCELLED
    )
    comp3 = get_component_by_name(job, "child2")
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, comp3.id).status
        == RuntimeState.CANCELLED
    )


def test_branch_skip_fan_in(tmp_path):
    """
    Fan-in cancellation:
      [ok_root, fail_root] --> join
    ok_root succeeds, fail_root fails, so join should be CANCELLED.
    """
    handler = JobExecutionHandler()
    config = {
        "name": "SkipFanInJob",
        "num_of_retries": 0,
        "file_logging": False,
        "metadata": {
            "created_by": 42,
            "created_at": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "ok_root",
                "comp_type": "test",
                "description": "",
                "next": ["join"],
            },
            {
                "name": "fail_root",
                "comp_type": "failtest",
                "description": "",
                "next": ["join"],
            },
            {
                "name": "join",
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

    # Job-level assertions
    assert mh.get_job_metrics(execution.id).status == RuntimeState.FAILED
    assert attempt.error is not None
    assert "fail stubcomponent failed" in attempt.error

    # ok_root ran, fail_root failed, join skipped
    comp1 = get_component_by_name(job, "ok_root")
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, comp1.id).status
        == RuntimeState.SUCCESS
    )
    comp2 = get_component_by_name(job, "fail_root")
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, comp2.id).status
        == RuntimeState.FAILED
    )
    comp3 = get_component_by_name(job, "join")
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, comp3.id).status
        == RuntimeState.CANCELLED
    )


def test_chain_skip_linear():
    """
    Chain cancellation:
      root --> middle --> leaf
    comp1 should FAIL, and both middle/leaf should be CANCELLED.
    """
    handler = JobExecutionHandler()
    config = {
        "name": "ChainSkipJob",
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
                "comp_type": "failtest",
                "description": "",
                "next": ["middle"],
            },
            {
                "name": "middle",
                "comp_type": "test",
                "description": "",
                "next": ["leaf"],
            },
            {
                "name": "leaf",
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

    # Job-level assertions
    assert mh.get_job_metrics(execution.id).status == RuntimeState.FAILED
    assert attempt.error is not None
    assert "fail stubcomponent failed" in attempt.error

    comp1 = get_component_by_name(job, "root")
    comp2 = get_component_by_name(job, "middle")
    comp3 = get_component_by_name(job, "leaf")
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, comp1.id).status
        == RuntimeState.FAILED
    )
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, comp2.id).status
        == RuntimeState.CANCELLED
    )
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, comp3.id).status
        == RuntimeState.CANCELLED
    )


def test_skip_diamond():
    """
    cancellation due to cancelled predecessor:
      a --> b / c -->d
    d should be CANCELLED since one predecessor was cancelled.
    """
    handler = JobExecutionHandler()
    config = {
        "name": "SkipDiamondJob",
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
                "comp_type": "failtest",
                "description": "",
                "next": ["b", "c"],
            },
            {
                "name": "b",
                "comp_type": "test",
                "description": "",
                "next": ["d"],
            },
            {
                "name": "c",
                "comp_type": "test",
                "description": "",
                "next": ["d"],
            },
            {
                "name": "d",
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

    # Job-level assertions
    assert mh.get_job_metrics(execution.id).status == RuntimeState.FAILED
    assert attempt.error is not None
    assert "fail stubcomponent failed" in attempt.error

    comp1 = get_component_by_name(job, "a")
    comp2 = get_component_by_name(job, "b")
    comp3 = get_component_by_name(job, "c")
    comp4 = get_component_by_name(job, "d")
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, comp1.id).status
        == RuntimeState.FAILED
    )
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, comp2.id).status
        == RuntimeState.CANCELLED
    )
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, comp3.id).status
        == RuntimeState.CANCELLED
    )
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, comp4.id).status
        == RuntimeState.CANCELLED
    )
