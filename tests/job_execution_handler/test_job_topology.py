from datetime import datetime

import etl_core.job_execution.runtimejob as runtimejob_module
from etl_core.components.runtime_state import RuntimeState
from etl_core.components.stubcomponents import StubComponent
from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from tests.helpers import get_component_by_name, runtime_job_from_config

# ensure Job._build_components() can find TestComponent
runtimejob_module.TestComponent = StubComponent


def test_fan_out_topology(schema_row_min):
    """
    root --> [child1, child2]
    All three should run to SUCCESS, and record metrics.lines_received == 1
    """
    handler = JobExecutionHandler()
    config = {
        "name": "FanOutJob",
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
                "comp_type": "test",
                "description": "",
                "routes": {"out": ["child1", "child2"]},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "child1",
                "comp_type": "test",
                "description": "",
                "routes": {"out": []},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "child2",
                "comp_type": "test",
                "description": "",
                "routes": {"out": []},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
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

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    comp1 = get_component_by_name(runtime_job, "root")
    comp2 = get_component_by_name(runtime_job, "child1")
    comp3 = get_component_by_name(runtime_job, "child2")

    comp1_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp1.id)
    comp2_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp2.id)
    comp3_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp3.id)

    assert comp1_metrics.lines_received == 1
    assert comp1_metrics.status == RuntimeState.SUCCESS
    assert comp2_metrics.lines_received == 1
    assert comp2_metrics.status == RuntimeState.SUCCESS
    assert comp3_metrics.lines_received == 1
    assert comp3_metrics.status == RuntimeState.SUCCESS


def test_fan_in_topology(schema_row_min):
    """
    [a, b] --> c
    a and b run in parallel, then c runs after both complete
    """
    handler = JobExecutionHandler()
    config = {
        "name": "FanInJob",
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
                "comp_type": "test",
                "description": "",
                "routes": {"out": ["c"]},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "b",
                "comp_type": "test",
                "description": "",
                "routes": {"out": ["c"]},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "c",
                "comp_type": "test",
                "description": "",
                "routes": {"out": []},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
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

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    comp1 = get_component_by_name(runtime_job, "a")
    comp2 = get_component_by_name(runtime_job, "b")
    comp3 = get_component_by_name(runtime_job, "c")

    comp1_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp1.id)
    comp2_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp2.id)
    comp3_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp3.id)

    assert comp1_metrics.lines_received == 1
    assert comp1_metrics.status == RuntimeState.SUCCESS
    assert comp2_metrics.lines_received == 1
    assert comp2_metrics.status == RuntimeState.SUCCESS
    assert comp3_metrics.lines_received == 2
    assert comp3_metrics.status == RuntimeState.SUCCESS


def test_diamond_topology(schema_row_min):
    """
    root --> [a, b] --> c
    A classic diamond: root fans out to a,b then joins at c
    """
    handler = JobExecutionHandler()
    config = {
        "name": "DiamondJob",
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
                "comp_type": "test",
                "description": "",
                "routes": {"out": ["a", "b"]},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "a",
                "comp_type": "test",
                "description": "",
                "routes": {"out": ["c"]},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "b",
                "comp_type": "test",
                "description": "",
                "routes": {"out": ["c"]},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
                "next": ["c"],
            },
            {
                "name": "c",
                "comp_type": "test",
                "description": "",
                "routes": {"out": []},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
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

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    comp1 = get_component_by_name(runtime_job, "root")
    comp2 = get_component_by_name(runtime_job, "a")
    comp3 = get_component_by_name(runtime_job, "b")
    comp4 = get_component_by_name(runtime_job, "c")

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
