from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from etl_core.components.runtime_state import RuntimeState
import etl_core.job_execution.runtimejob as runtimejob_module
from etl_core.components.stubcomponents import StubComponent
from tests.helpers import get_component_by_name, runtime_job_from_config
from datetime import datetime

# ensure Job._build_components() can find TestComponent
runtimejob_module.TestComponent = StubComponent


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
            "user_id": 42,
            "timestamp": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "root",
                "comp_type": "failtest",  # will throw
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
                "next": ["child1", "child2"],
            },
            {
                "name": "child1",
                "comp_type": "test",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "child2",
                "comp_type": "test",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
        ],
    }

    runtime_job = runtime_job_from_config(config)
    execution = handler.execute_job(runtime_job)
    attempt = execution.attempts[0]
    assert len(execution.attempts) == 1
    mh = handler.job_info.metrics_handler

    # Job-level assertions
    assert mh.get_job_metrics(execution.id).status == RuntimeState.FAILED
    assert attempt.error is not None
    assert "fail stubcomponent failed" in attempt.error

    # Component statuses
    comp1 = get_component_by_name(runtime_job, "root")
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, comp1.id).status
        == RuntimeState.FAILED
    )
    comp2 = get_component_by_name(runtime_job, "child1")
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, comp2.id).status
        == RuntimeState.CANCELLED
    )
    comp3 = get_component_by_name(runtime_job, "child2")
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
            "user_id": 42,
            "timestamp": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "ok_root",
                "comp_type": "test",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
                "next": ["join"],
            },
            {
                "name": "fail_root",
                "comp_type": "failtest",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
                "next": ["join"],
            },
            {
                "name": "join",
                "comp_type": "test",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
        ],
    }

    runtime_job = runtime_job_from_config(config)
    execution = handler.execute_job(runtime_job)
    attempt = execution.attempts[0]
    assert len(execution.attempts) == 1
    mh = handler.job_info.metrics_handler

    # Job-level assertions
    assert mh.get_job_metrics(execution.id).status == RuntimeState.FAILED
    assert attempt.error is not None
    assert "fail stubcomponent failed" in attempt.error

    # ok_root ran, fail_root failed, join skipped
    comp1 = get_component_by_name(runtime_job, "ok_root")
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, comp1.id).status
        == RuntimeState.SUCCESS
    )
    comp2 = get_component_by_name(runtime_job, "fail_root")
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, comp2.id).status
        == RuntimeState.FAILED
    )
    comp3 = get_component_by_name(runtime_job, "join")
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
            "user_id": 42,
            "timestamp": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "root",
                "comp_type": "failtest",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
                "next": ["middle"],
            },
            {
                "name": "middle",
                "comp_type": "test",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
                "next": ["leaf"],
            },
            {
                "name": "leaf",
                "comp_type": "test",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
        ],
    }

    runtime_job = runtime_job_from_config(config)
    execution = handler.execute_job(runtime_job)
    attempt = execution.attempts[0]
    assert len(execution.attempts) == 1
    mh = handler.job_info.metrics_handler

    # Job-level assertions
    assert mh.get_job_metrics(execution.id).status == RuntimeState.FAILED
    assert attempt.error is not None
    assert "fail stubcomponent failed" in attempt.error

    comp1 = get_component_by_name(runtime_job, "root")
    comp2 = get_component_by_name(runtime_job, "middle")
    comp3 = get_component_by_name(runtime_job, "leaf")
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
            "user_id": 42,
            "timestamp": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "a",
                "comp_type": "failtest",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
                "next": ["b", "c"],
            },
            {
                "name": "b",
                "comp_type": "test",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
                "next": ["d"],
            },
            {
                "name": "c",
                "comp_type": "test",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
                "next": ["d"],
            },
            {
                "name": "d",
                "comp_type": "test",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
        ],
    }

    runtime_job = runtime_job_from_config(config)
    execution = handler.execute_job(runtime_job)
    attempt = execution.attempts[0]
    assert len(execution.attempts) == 1
    mh = handler.job_info.metrics_handler

    # Job-level assertions
    assert mh.get_job_metrics(execution.id).status == RuntimeState.FAILED
    assert attempt.error is not None
    assert "fail stubcomponent failed" in attempt.error

    comp1 = get_component_by_name(runtime_job, "a")
    comp2 = get_component_by_name(runtime_job, "b")
    comp3 = get_component_by_name(runtime_job, "c")
    comp4 = get_component_by_name(runtime_job, "d")
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
