from datetime import datetime

import etl_core.job_execution.runtimejob as runtimejob_module
from etl_core.components.runtime_state import RuntimeState
from etl_core.components.stubcomponents import StubComponent
from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from tests.helpers import get_component_by_name, runtime_job_from_config


# ensure Job._build_components() can find TestComponent
runtimejob_module.TestComponent = StubComponent


def test_execute_job_single_test_component(schema_row_min):
    """
    A job with one TestComponent should:
      - run to COMPLETED
      - record exactly one JobExecution
      - record component.metrics.lines_received == 1
    """
    handler = JobExecutionHandler()
    config = {
        "name": "ExecuteTestComponent",
        "num_of_retries": 0,
        "file_logging": False,
        "metadata": {
            "user_id": 42,
            "timestamp": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "test1",
                "comp_type": "test",
                "description": "a test comp",
                "routes": {"out": []},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            }
        ],
    }

    runtime_job = runtime_job_from_config(config)

    execution = handler.execute_job(runtime_job)
    attempt = execution.attempts[0]
    assert len(execution.attempts) == 1
    mh = handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS
    comp = get_component_by_name(runtime_job, "test1")

    comp_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp.id)
    assert comp_metrics.status == RuntimeState.SUCCESS
    assert comp_metrics.lines_received == 1


def test_execute_job_chain_components_file_logging(schema_row_min):
    """
    A job with two chained TestComponents should:
      - run to COMPLETED
      - record a single execution
      - record metrics for both components
      - exercise the file_logging branch
    """
    handler = JobExecutionHandler()
    config = {
        "job_name": "ChainJob",
        "num_of_retries": 0,
        "file_logging": True,
        "metadata": {
            "user_id": 42,
            "timestamp": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "comp1",
                "comp_type": "test",
                "description": "first",
                "routes": {"out": ["comp2"]},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "comp2",
                "comp_type": "test",
                "description": "second",
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

    assert runtime_job.file_logging is True
    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    comp1 = get_component_by_name(runtime_job, "comp1")
    comp2 = get_component_by_name(runtime_job, "comp2")

    assert mh.get_comp_metrics(execution.id, attempt.id, comp1.id).lines_received == 1
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, comp1.id).status
        == RuntimeState.SUCCESS
    )
    assert mh.get_comp_metrics(execution.id, attempt.id, comp2.id).lines_received == 1
    assert (
        mh.get_comp_metrics(execution.id, attempt.id, comp2.id).status
        == RuntimeState.SUCCESS
    )


def test_execute_job_failing_and_cancelled_components(schema_row_min):
    """
    A job with a failing first component and a dependent second component should:
      - end up FAILED
      - set job.error appropriately
      - record no executions
      - mark first component FAILED
      - mark second component CANCELLED
    """
    handler = JobExecutionHandler()
    config = {
        "job_name": "ChainErrorJob",
        "num_of_retries": 0,
        "file_logging": False,
        "metadata": {
            "user_id": 42,
            "timestamp": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "comp1",
                "comp_type": "failtest",
                "description": "will fail",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
                "routes": {"out": ["comp2"]},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
            },
            {
                "name": "comp2",
                "comp_type": "test",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
                "description": "should be cancelled",
                "routes": {"out": []},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
            },
        ],
    }

    runtime_job = runtime_job_from_config(config)
    execution = handler.execute_job(runtime_job)
    attempt = execution.attempts[0]
    assert len(execution.attempts) == 1
    mh = handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.FAILED
    assert attempt.error is not None
    assert "fail stubcomponent failed" in attempt.error

    comp1 = get_component_by_name(runtime_job, "comp1")
    comp2 = get_component_by_name(runtime_job, "comp2")
    comp1_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp1.id)
    comp2_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp2.id)
    assert comp1_metrics.status == RuntimeState.FAILED
    assert comp2_metrics.status == RuntimeState.CANCELLED


def test_retry_logic_and_metrics(schema_row_min):
    handler = JobExecutionHandler()
    config = {
        "job_name": "RetryOnceJob",
        "num_of_retries": 1,
        "file_logging": False,
        "metadata": {
            "user_id": 42,
            "timestamp": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "c1",
                "comp_type": "stub_fail_once",
                "description": "",
                "routes": {"out": []},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            }
        ],
    }
    runtime_job = runtime_job_from_config(config)
    execution = handler.execute_job(runtime_job)
    attempt = execution.attempts[1]
    assert len(execution.attempts) == 2
    mh = handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS
    comp = get_component_by_name(runtime_job, "c1")
    assert mh.get_comp_metrics(execution.id, attempt.id, comp.id).lines_received == 1


def test_execute_job_linear_chain(schema_row_min):
    """
    A job with four chained TestComponents should:
      - run to COMPLETED
      - record exactly one JobExecution
      - record metrics for all four components
      - mark all components COMPLETED
    """
    handler = JobExecutionHandler()
    config = {
        "job_name": "LinearChain",
        "num_of_retries": 0,
        "file_logging": False,
        "metadata": {
            "user_id": 42,
            "timestamp": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "c1",
                "comp_type": "test",
                "description": "",
                "routes": {"out": ["c2"]},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "c2",
                "comp_type": "test",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
                "routes": {"out": ["c3"]},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
            },
            {
                "name": "c3",
                "comp_type": "test",
                "description": "",
                "routes": {"out": ["c4"]},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "c4",
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

    comp1 = get_component_by_name(runtime_job, "c1")
    comp2 = get_component_by_name(runtime_job, "c2")
    comp3 = get_component_by_name(runtime_job, "c3")
    comp4 = get_component_by_name(runtime_job, "c4")

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
    assert comp4_metrics.lines_received == 1
    assert comp4_metrics.status == RuntimeState.SUCCESS


def test_execute_linear_chain_with_retry_metrics(schema_row_min):
    """
    A linear job with two components where:
      - the first component fails once, then succeeds on retry
      - first attempt: comp1 FAILED (0 lines), comp2 CANCELLED (0 lines)
      - second attempt: both SUCCESS (1 line each)
      - final job status is SUCCESS
    """
    handler = JobExecutionHandler()
    config = {
        "name": "LinearRetryJob",
        "num_of_retries": 1,
        "file_logging": False,
        "metadata": {
            "user_id": 42,
            "timestamp": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "c1",
                "comp_type": "stub_fail_once",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
                "routes": {"out": ["c2"]},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
            },
            {
                "name": "c2",
                "comp_type": "test",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
                "routes": {"out": ["c3"]},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
            },
            {
                "name": "c3",
                "comp_type": "test",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
            },
        ],
    }
    runtime_job = runtime_job_from_config(config)
    execution = handler.execute_job(runtime_job)

    assert len(execution.attempts) == 2
    mh = handler.job_info.metrics_handler

    comp1 = get_component_by_name(runtime_job, "c1")
    comp2 = get_component_by_name(runtime_job, "c2")
    comp3 = get_component_by_name(runtime_job, "c3")
    first = execution.attempts[0]
    second = execution.attempts[1]

    m1_first = mh.get_comp_metrics(execution.id, first.id, comp1.id)
    m2_first = mh.get_comp_metrics(execution.id, first.id, comp2.id)
    m3_first = mh.get_comp_metrics(execution.id, first.id, comp3.id)
    assert m1_first.status == RuntimeState.FAILED
    assert m1_first.lines_received == 0
    assert m2_first.status == RuntimeState.CANCELLED
    assert m2_first.lines_received == 0
    assert m3_first.status == RuntimeState.CANCELLED
    assert m3_first.lines_received == 0

    m1_second = mh.get_comp_metrics(execution.id, second.id, comp1.id)
    m2_second = mh.get_comp_metrics(execution.id, second.id, comp2.id)
    m3_second = mh.get_comp_metrics(execution.id, second.id, comp3.id)
    assert m1_second.status == RuntimeState.SUCCESS
    assert m1_second.lines_received == 1
    assert m2_second.status == RuntimeState.SUCCESS
    assert m2_second.lines_received == 1
    assert m3_second.status == RuntimeState.SUCCESS
    assert m3_second.lines_received == 1

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS
