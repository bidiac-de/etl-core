from src.job_execution.job_execution_handler import JobExecutionHandler, Job
from src.components.runtime_state import RuntimeState
from tests.helpers import get_component_by_name
import src.job_execution.job as job_module
from src.components.stubcomponents import MultiSource, MultiEcho

# ensure Job can resolve these stub types for the tests
job_module.MultiSourceComponent = MultiSource
job_module.MultiEchoComponent = MultiEcho


def _schema():
    return {"fields": [{"name": "id", "data_type": "integer", "nullable": False}]}


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
                "routes": {"out": ["echo"]},
                "port_schemas": {"out": _schema()},
            },
            {
                "name": "echo",
                "comp_type": "multi_echo",
                "description": "",
                "routes": {"out": []},
                "in_port_schemas": {"in": _schema()},
                "port_schemas": {"out": _schema()},
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
                "routes": {"out": ["echo1", "echo2"]},
                "port_schemas": {"out": _schema()},
            },
            {
                "name": "echo1",
                "comp_type": "multi_echo",
                "description": "",
                "routes": {"out": []},
                "in_port_schemas": {"in": _schema()},
                "port_schemas": {"out": _schema()},
            },
            {
                "name": "echo2",
                "comp_type": "multi_echo",
                "description": "",
                "routes": {"out": []},
                "in_port_schemas": {"in": _schema()},
                "port_schemas": {"out": _schema()},
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
                "routes": {"out": ["echo"]},
                "port_schemas": {"out": _schema()},
            },
            {
                "name": "src2",
                "comp_type": "multi_source",
                "description": "",
                "routes": {"out": ["echo"]},
                "port_schemas": {"out": _schema()},
            },
            {
                "name": "echo",
                "comp_type": "multi_echo",
                "description": "",
                "routes": {"out": []},
                "in_port_schemas": {"in": _schema()},
                "port_schemas": {"out": _schema()},
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

    src1_metrics = mh.get_comp_metrics(execution.id, attempt.id, src1.id)
    src2_metrics = mh.get_comp_metrics(execution.id, attempt.id, src2.id)
    echo_metrics = mh.get_comp_metrics(execution.id, attempt.id, echo.id)

    assert src1_metrics.lines_received == src1.count
    assert src2_metrics.lines_received == src2.count
    assert echo_metrics.lines_received == src1.count + src2.count
