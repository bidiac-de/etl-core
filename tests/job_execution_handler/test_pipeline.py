from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from etl_core.components.runtime_state import RuntimeState
from tests.helpers import get_component_by_name, runtime_job_from_config
import etl_core.job_execution.runtimejob as runtimejob_module
from etl_core.components.stubcomponents import MultiSource, MultiEcho
from datetime import datetime

# ensure Job._build_components() can find TestComponent
runtimejob_module.MultiSourceComponent = MultiSource
runtimejob_module.MultiEchoComponent = MultiEcho


def test_linear_stream_multiple_rows():
    handler = JobExecutionHandler()
    config = {
        "name": "LinearStreamJob",
        "num_of_retries": 0,
        "file_logging": False,
        "strategy_type": "row",
        "metadata": {
            "user_id": 42,
            "timestamp": datetime.now(),
        },
        "components": [
            {
                "name": "source",
                "comp_type": "multi_source",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
                "next": ["echo"],
            },
            {
                "name": "echo",
                "comp_type": "multi_echo",
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
    mh = handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    src_comp = get_component_by_name(runtime_job, "source")
    echo_comp = get_component_by_name(runtime_job, "echo")

    src_metrics = mh.get_comp_metrics(execution.id, attempt.id, src_comp.id)
    echo_metrics = mh.get_comp_metrics(execution.id, attempt.id, echo_comp.id)

    assert src_metrics.lines_received == src_comp.count
    assert src_metrics.status == RuntimeState.SUCCESS

    assert echo_metrics.lines_received == src_comp.count
    assert echo_metrics.status == RuntimeState.SUCCESS


def test_fan_out_multiple_rows():
    handler = JobExecutionHandler()
    config = {
        "name": "FanOutStreamJob",
        "num_of_retries": 0,
        "file_logging": False,
        "strategy_type": "row",
        "metadata": {
            "user_id": 42,
            "timestamp": datetime.now(),
        },
        "components": [
            {
                "name": "source",
                "comp_type": "multi_source",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
                "next": ["echo1", "echo2"],
            },
            {
                "name": "echo1",
                "comp_type": "multi_echo",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "echo2",
                "comp_type": "multi_echo",
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
    mh = handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    source = get_component_by_name(runtime_job, "source")
    echo1 = get_component_by_name(runtime_job, "echo1")
    echo2 = get_component_by_name(runtime_job, "echo2")

    src_metrics = mh.get_comp_metrics(execution.id, attempt.id, source.id)
    e1_metrics = mh.get_comp_metrics(execution.id, attempt.id, echo1.id)
    e2_metrics = mh.get_comp_metrics(execution.id, attempt.id, echo2.id)

    assert src_metrics.lines_received == source.count
    assert e1_metrics.lines_received == source.count
    assert e2_metrics.lines_received == source.count


def test_fan_in_multiple_rows():
    handler = JobExecutionHandler()
    config = {
        "name": "FanInStreamJob",
        "num_of_retries": 0,
        "file_logging": False,
        "strategy_type": "row",
        "metadata": {
            "user_id": 42,
            "timestamp": datetime.now(),
        },
        "components": [
            {
                "name": "src1",
                "comp_type": "multi_source",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
                "next": ["echo"],
            },
            {
                "name": "src2",
                "comp_type": "multi_source",
                "description": "",
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
                "next": ["echo"],
            },
            {
                "name": "echo",
                "comp_type": "multi_echo",
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
    mh = handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    src1 = get_component_by_name(runtime_job, "src1")
    src2 = get_component_by_name(runtime_job, "src2")
    echo = get_component_by_name(runtime_job, "echo")

    e_metrics = mh.get_comp_metrics(execution.id, attempt.id, echo.id)
    expected = src1.count + src2.count

    assert e_metrics.lines_received == expected
    assert e_metrics.status == RuntimeState.SUCCESS
