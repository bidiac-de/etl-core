from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from etl_core.components.runtime_state import RuntimeState
import etl_core.job_execution.runtimejob as runtimejob_module
from etl_core.components.stubcomponents import StubComponent
from tests.helpers import get_component_by_name, runtime_job_from_config
from datetime import datetime

# ensure Job._build_components() can find TestComponent
runtimejob_module.TestComponent = StubComponent

def _schema() -> dict:
    # minimal single-field schema used across skip tests
    return {"fields": [{"name": "id", "data_type": "integer", "nullable": False}]}


def test_branch_skip_fan_out() -> None:
    """
    Fan-out cancellation:
      failtest --> [child1, child2]
    root fails, both children are CANCELLED.
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
                "comp_type": "failtest",
                "description": "",
                "routes": {"out": ["child1", "child2"]},
                "out_port_schemas": {"out": _schema()},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "child1",
                "comp_type": "test",
                "description": "",
                "in_port_schemas": {"in": _schema()},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "child2",
                "comp_type": "test",
                "description": "",
                "in_port_schemas": {"in": _schema()},
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

    # Job failed and root failed
    assert mh.get_job_metrics(execution.id).status == RuntimeState.FAILED
    assert attempt.error is not None
    assert "fail stubcomponent failed" in attempt.error

    root = get_component_by_name(job, "root")
    child1 = get_component_by_name(job, "child1")
    child2 = get_component_by_name(job, "child2")

    assert (
        mh.get_comp_metrics(execution.id, attempt.id, root.id).status
        == RuntimeState.FAILED
    )
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, child1.id).status
        == RuntimeState.CANCELLED
    )
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, child2.id).status
        == RuntimeState.CANCELLED
    )


def test_branch_skip_fan_in() -> None:
    """
    Fan-in cancellation:
      [ok_root, fail_root] --> join
    ok_root succeeds, fail_root fails, join is CANCELLED.
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
                "routes": {"out": ["join"]},
                "out_port_schemas": {"out": _schema()},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "fail_root",
                "comp_type": "failtest",
                "description": "",
                "routes": {"out": ["join"]},
                "out_port_schemas": {"out": _schema()},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "join",
                "comp_type": "test",
                "description": "",
                "in_port_schemas": {"in": _schema()},
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

    assert mh.get_job_metrics(execution.id).status == RuntimeState.FAILED
    assert attempt.error and "fail stubcomponent failed" in attempt.error

    ok_root = get_component_by_name(job, "ok_root")
    fail_root = get_component_by_name(job, "fail_root")
    join = get_component_by_name(job, "join")

    assert (
        mh.get_comp_metrics(execution.id, attempt.id, ok_root.id).status
        == RuntimeState.SUCCESS
    )
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, fail_root.id).status
        == RuntimeState.FAILED
    )
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, join.id).status
        == RuntimeState.CANCELLED
    )


def test_chain_skip_linear() -> None:
    """
    Chain cancellation:
      root --> middle --> leaf
    root fails; middle and leaf are CANCELLED.
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
                "routes": {"out": ["middle"]},
                "out_port_schemas": {"out": _schema()},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "middle",
                "comp_type": "test",
                "description": "",
                "routes": {"out": ["leaf"]},
                "in_port_schemas": {"in": _schema()},
                "out_port_schemas": {"out": _schema()},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "leaf",
                "comp_type": "test",
                "description": "",
                "in_port_schemas": {"in": _schema()},
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

    assert mh.get_job_metrics(execution.id).status == RuntimeState.FAILED
    assert attempt.error and "fail stubcomponent failed" in attempt.error

    root = get_component_by_name(job, "root")
    middle = get_component_by_name(job, "middle")
    leaf = get_component_by_name(job, "leaf")

    assert (
        mh.get_comp_metrics(execution.id, attempt.id, root.id).status
        == RuntimeState.FAILED
    )
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, middle.id).status
        == RuntimeState.CANCELLED
    )
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, leaf.id).status
        == RuntimeState.CANCELLED
    )


def test_skip_diamond() -> None:
    """
    cancellation due to cancelled predecessor:
      a --> b / c --> d
    d is CANCELLED since one predecessor was cancelled.
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
                "routes": {"out": ["b", "c"]},
                "out_port_schemas": {"out": _schema()},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "b",
                "comp_type": "test",
                "description": "",
                "routes": {"out": ["d"]},
                "in_port_schemas": {"in": _schema()},
                "out_port_schemas": {"out": _schema()},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "c",
                "comp_type": "test",
                "description": "",
                "routes": {"out": ["d"]},
                "in_port_schemas": {"in": _schema()},
                "out_port_schemas": {"out": _schema()},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "d",
                "comp_type": "test",
                "description": "",
                "in_port_schemas": {"in": _schema()},
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
    assert attempt.error and "fail stubcomponent failed" in attempt.error

    a = get_component_by_name(job, "a")
    b = get_component_by_name(job, "b")
    c = get_component_by_name(job, "c")
    d = get_component_by_name(job, "d")

    assert (
        mh.get_comp_metrics(execution.id, attempt.id, a.id).status
        == RuntimeState.FAILED
    )
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, b.id).status
        == RuntimeState.CANCELLED
    )
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, c.id).status
        == RuntimeState.CANCELLED
    )
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, d.id).status
        == RuntimeState.CANCELLED
    )
