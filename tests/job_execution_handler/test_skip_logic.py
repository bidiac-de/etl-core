# tests/job_execution_handler/test_skip_logic.py
from datetime import datetime

from src.components.runtime_state import RuntimeState
from src.job_execution.job import Job
from src.job_execution.job_execution_handler import JobExecutionHandler
from tests.helpers import get_component_by_name


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
        "metadata": {"created_by": 42, "created_at": datetime.now()},
        "strategy_type": "row",
        "components": [
            {
                "name": "root",
                "comp_type": "failtest",
                "description": "",
                "routes": {"out": ["child1", "child2"]},
                "out_port_schemas": {"out": _schema()},
            },
            {
                "name": "child1",
                "comp_type": "test",
                "description": "",
                "in_port_schemas": {"in": _schema()},
            },
            {
                "name": "child2",
                "comp_type": "test",
                "description": "",
                "in_port_schemas": {"in": _schema()},
            },
        ],
    }

    job = Job(**config)
    execution = handler.execute_job(job)
    attempt = execution.attempts[0]
    mh = handler.job_info.metrics_handler

    # Job failed and root failed
    assert mh.get_job_metrics(execution.id).status == RuntimeState.FAILED
    assert attempt.error and "fail stubcomponent failed" in attempt.error

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
        "metadata": {"created_by": 42, "created_at": datetime.now()},
        "strategy_type": "row",
        "components": [
            {
                "name": "ok_root",
                "comp_type": "test",
                "description": "",
                "routes": {"out": ["join"]},
                "out_port_schemas": {"out": _schema()},
            },
            {
                "name": "fail_root",
                "comp_type": "failtest",
                "description": "",
                "routes": {"out": ["join"]},
                "out_port_schemas": {"out": _schema()},
            },
            {
                "name": "join",
                "comp_type": "test",
                "description": "",
                "in_port_schemas": {"in": _schema()},
            },
        ],
    }

    job = Job(**config)
    execution = handler.execute_job(job)
    attempt = execution.attempts[0]
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
        "metadata": {"created_by": 42, "created_at": datetime.now()},
        "strategy_type": "row",
        "components": [
            {
                "name": "root",
                "comp_type": "failtest",
                "description": "",
                "routes": {"out": ["middle"]},
                "out_port_schemas": {"out": _schema()},
            },
            {
                "name": "middle",
                "comp_type": "test",
                "description": "",
                "routes": {"out": ["leaf"]},
                "in_port_schemas": {"in": _schema()},
                "out_port_schemas": {"out": _schema()},
            },
            {
                "name": "leaf",
                "comp_type": "test",
                "description": "",
                "in_port_schemas": {"in": _schema()},
            },
        ],
    }

    job = Job(**config)
    execution = handler.execute_job(job)
    attempt = execution.attempts[0]
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
        "metadata": {"created_by": 42, "created_at": datetime.now()},
        "strategy_type": "row",
        "components": [
            {
                "name": "a",
                "comp_type": "failtest",
                "description": "",
                "routes": {"out": ["b", "c"]},
                "out_port_schemas": {"out": _schema()},
            },
            {
                "name": "b",
                "comp_type": "test",
                "description": "",
                "routes": {"out": ["d"]},
                "in_port_schemas": {"in": _schema()},
                "out_port_schemas": {"out": _schema()},
            },
            {
                "name": "c",
                "comp_type": "test",
                "description": "",
                "routes": {"out": ["d"]},
                "in_port_schemas": {"in": _schema()},
                "out_port_schemas": {"out": _schema()},
            },
            {
                "name": "d",
                "comp_type": "test",
                "description": "",
                "in_port_schemas": {"in": _schema()},
            },
        ],
    }

    job = Job(**config)
    execution = handler.execute_job(job)
    attempt = execution.attempts[0]
    mh = handler.job_info.metrics_handler

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
