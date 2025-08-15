from src.job_execution.job_execution_handler import JobExecutionHandler, Job
from src.components.runtime_state import RuntimeState
from tests.helpers import get_component_by_name
import src.job_execution.job as job_module
from src.components.stubcomponents import MultiSource, MultiEcho

# ensure Job._build_components() can find TestComponent
job_module.MultiSourceComponent = MultiSource
job_module.MultiEchoComponent = MultiEcho


def test_linear_stream_multiple_rows():
    handler = JobExecutionHandler()
    config = {
        "job_name": "LinearStreamJob",
        "num_of_retries": 0,
        "file_logging": False,
        "strategy_type": "row",
        "components": [
            {
                "name": "source",
                "comp_type": "multi_source",
                "description": "",
                "next": ["echo"],
                "in_schema": {
                    "fields": [
                        {"name": "id", "data_type": "integer", "nullable": False}
                    ]
                },
                "out_schema": {
                    "fields": [
                        {"name": "id", "data_type": "integer", "nullable": False}
                    ]
                },
            },
            {
                "name": "echo",
                "comp_type": "multi_echo",
                "description": "",
                "in_schema": {
                    "fields": [
                        {"name": "id", "data_type": "integer", "nullable": False}
                    ]
                },
                "out_schema": {
                    "fields": [
                        {"name": "id", "data_type": "integer", "nullable": False}
                    ]
                },
            },
        ],
    }
    job = Job(**config)
    execution = handler.execute_job(job)
    attempt = execution.attempts[0]
    mh = handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    src_comp = get_component_by_name(job, "source")
    echo_comp = get_component_by_name(job, "echo")

    src_metrics = mh.get_comp_metrics(execution.id, attempt.id, src_comp.id)
    echo_metrics = mh.get_comp_metrics(execution.id, attempt.id, echo_comp.id)

    assert src_metrics.lines_received == src_comp.count
    assert src_metrics.status == RuntimeState.SUCCESS

    assert echo_metrics.lines_received == src_comp.count
    assert echo_metrics.status == RuntimeState.SUCCESS


def test_fan_out_multiple_rows():
    handler = JobExecutionHandler()
    config = {
        "job_name": "FanOutStreamJob",
        "num_of_retries": 0,
        "file_logging": False,
        "strategy_type": "row",
        "components": [
            {
                "name": "source",
                "comp_type": "multi_source",
                "description": "",
                "next": ["echo1", "echo2"],
                "in_schema": {
                    "fields": [
                        {"name": "id", "data_type": "integer", "nullable": False}
                    ]
                },
                "out_schema": {
                    "fields": [
                        {"name": "id", "data_type": "integer", "nullable": False}
                    ]
                },
            },
            {
                "name": "echo1",
                "comp_type": "multi_echo",
                "description": "",
                "in_schema": {
                    "fields": [
                        {"name": "id", "data_type": "integer", "nullable": False}
                    ]
                },
                "out_schema": {
                    "fields": [
                        {"name": "id", "data_type": "integer", "nullable": False}
                    ]
                },
            },
            {
                "name": "echo2",
                "comp_type": "multi_echo",
                "description": "",
                "in_schema": {
                    "fields": [
                        {"name": "id", "data_type": "integer", "nullable": False}
                    ]
                },
                "out_schema": {
                    "fields": [
                        {"name": "id", "data_type": "integer", "nullable": False}
                    ]
                },
            },
        ],
    }
    job = Job(**config)
    execution = handler.execute_job(job)
    attempt = execution.attempts[0]
    mh = handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    source = get_component_by_name(job, "source")
    echo1 = get_component_by_name(job, "echo1")
    echo2 = get_component_by_name(job, "echo2")

    src_metrics = mh.get_comp_metrics(execution.id, attempt.id, source.id)
    e1_metrics = mh.get_comp_metrics(execution.id, attempt.id, echo1.id)
    e2_metrics = mh.get_comp_metrics(execution.id, attempt.id, echo2.id)

    assert src_metrics.lines_received == source.count
    assert e1_metrics.lines_received == source.count
    assert e2_metrics.lines_received == source.count


def test_fan_in_multiple_rows():
    handler = JobExecutionHandler()
    config = {
        "job_name": "FanInStreamJob",
        "num_of_retries": 0,
        "file_logging": False,
        "strategy_type": "row",
        "components": [
            {
                "name": "src1",
                "comp_type": "multi_source",
                "description": "",
                "next": ["echo"],
                "in_schema": {
                    "fields": [
                        {"name": "id", "data_type": "integer", "nullable": False}
                    ]
                },
                "out_schema": {
                    "fields": [
                        {"name": "id", "data_type": "integer", "nullable": False}
                    ]
                },
            },
            {
                "name": "src2",
                "comp_type": "multi_source",
                "description": "",
                "next": ["echo"],
                "in_schema": {
                    "fields": [
                        {"name": "id", "data_type": "integer", "nullable": False}
                    ]
                },
                "out_schema": {
                    "fields": [
                        {"name": "id", "data_type": "integer", "nullable": False}
                    ]
                },
            },
            {
                "name": "echo",
                "comp_type": "multi_echo",
                "description": "",
                "in_schema": {
                    "fields": [
                        {"name": "id", "data_type": "integer", "nullable": False}
                    ]
                },
                "out_schema": {
                    "fields": [
                        {"name": "id", "data_type": "integer", "nullable": False}
                    ]
                },
            },
        ],
    }

    job = Job(**config)
    execution = handler.execute_job(job)
    attempt = execution.attempts[0]
    mh = handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    src1 = get_component_by_name(job, "src1")
    src2 = get_component_by_name(job, "src2")
    echo = get_component_by_name(job, "echo")

    e_metrics = mh.get_comp_metrics(execution.id, attempt.id, echo.id)
    expected = src1.count + src2.count

    assert e_metrics.lines_received == expected
    assert e_metrics.status == RuntimeState.SUCCESS
