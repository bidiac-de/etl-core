import asyncio
from datetime import datetime

import etl_core.job_execution.runtimejob as runtimejob_module
from etl_core.job_execution.runtimejob import RuntimeJob
from etl_core.components.runtime_state import RuntimeState
from etl_core.components.stubcomponents import MultiSource, MultiEcho
from etl_core.components.data_operations.schema_mapping.schema_mapping_component import (  # noqa: E501
    SchemaMappingComponent,
)
from etl_core.components.data_operations.schema_mapping.join_rules import (
    JoinPlan,
    JoinStep,
)
from etl_core.components.envelopes import Out
from etl_core.components.wiring.ports import EdgeRef
from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from tests.helpers import get_component_by_name, runtime_job_from_config

# Replace runtime job component references with deterministic test doubles for these tests.
runtimejob_module.MultiSourceComponent = MultiSource
runtimejob_module.MultiEchoComponent = MultiEcho


def test_linear_stream_multiple_rows(schema_row_min):
    handler = JobExecutionHandler()
    config = {
        "name": "LinearStreamJob",
        "num_of_retries": 0,
        "file_logging": False,
        "strategy_type": "row",
        "metadata": {"user_id": 42, "timestamp": datetime.now()},
        "components": [
            {
                "name": "source",
                "comp_type": "multi_source",
                "description": "",
                "routes": {"out": ["echo"]},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {"user_id": 42, "timestamp": datetime.now()},
            },
            {
                "name": "echo",
                "comp_type": "multi_echo",
                "description": "",
                "routes": {"out": []},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {"user_id": 42, "timestamp": datetime.now()},
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


def test_fan_out_multiple_rows(schema_row_min):
    handler = JobExecutionHandler()
    config = {
        "name": "FanOutStreamJob",
        "num_of_retries": 0,
        "file_logging": False,
        "strategy_type": "row",
        "metadata": {"user_id": 42, "timestamp": datetime.now()},
        "components": [
            {
                "name": "source",
                "comp_type": "multi_source",
                "description": "",
                "routes": {"out": ["echo1", "echo2"]},
                "out_port_schemas": {"out": schema_row_min},
            },
            {
                "name": "echo1",
                "comp_type": "multi_echo",
                "description": "",
                "routes": {"out": []},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
            },
            {
                "name": "echo2",
                "comp_type": "multi_echo",
                "description": "",
                "routes": {"out": []},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {"user_id": 42, "timestamp": datetime.now()},
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


def test_fan_in_multiple_rows(schema_row_min):
    handler = JobExecutionHandler()
    config = {
        "name": "FanInStreamJob",
        "num_of_retries": 0,
        "file_logging": False,
        "strategy_type": "row",
        "metadata": {"user_id": 42, "timestamp": datetime.now()},
        "components": [
            {
                "name": "src1",
                "comp_type": "multi_source",
                "description": "",
                "routes": {"out": ["echo"]},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {"user_id": 42, "timestamp": datetime.now()},
            },
            {
                "name": "src2",
                "comp_type": "multi_source",
                "description": "",
                "routes": {"out": ["echo"]},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {"user_id": 42, "timestamp": datetime.now()},
            },
            {
                "name": "echo",
                "comp_type": "multi_echo",
                "description": "",
                "routes": {"out": []},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {"user_id": 42, "timestamp": datetime.now()},
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

    src1_metrics = mh.get_comp_metrics(execution.id, attempt.id, src1.id)
    src2_metrics = mh.get_comp_metrics(execution.id, attempt.id, src2.id)
    echo_metrics = mh.get_comp_metrics(execution.id, attempt.id, echo.id)

    assert src1_metrics.lines_received == src1.count
    assert src2_metrics.lines_received == src2.count
    assert echo_metrics.lines_received == src1.count + src2.count


def test_single_predecessor_multi_port_join(schema_row_min):
    handler = JobExecutionHandler()
    source = MultiSource(
        name="source",
        comp_type="multi_source",
        description="",
        routes={"out": [EdgeRef(to="fanout", in_port="in")]},
        out_port_schemas={"out": schema_row_min},
        count=3,
    )
    fanout = MultiEcho(
        name="fanout",
        comp_type="multi_echo",
        description="",
        in_port_schemas={"in": schema_row_min},
        out_port_schemas={"out": schema_row_min},
        routes={
            "out": [
                EdgeRef(to="join", in_port="L"),
                EdgeRef(to="join", in_port="R"),
            ]
        },
    )
    join = SchemaMappingComponent(
        name="join",
        comp_type="schema_mapping",
        description="",
        extra_input_ports=["L", "R"],
        in_port_schemas={"L": schema_row_min, "R": schema_row_min},
        extra_output_ports=["J"],
        out_port_schemas={"J": schema_row_min},
        routes={"J": [EdgeRef(to="sink", in_port="in")]},
        join_plan=JoinPlan(
            steps=[
                JoinStep(
                    left_port="L",
                    right_port="R",
                    left_on="id",
                    right_on="id",
                    how="inner",
                    output_port="J",
                )
            ]
        ),
    )
    sink = MultiEcho(
        name="sink",
        comp_type="multi_echo",
        description="",
        in_port_schemas={"in": schema_row_min},
        out_port_schemas={"out": schema_row_min},
        routes={"out": []},
    )

    runtime_job = RuntimeJob(
        name="SplitJoinJob",
        num_of_retries=0,
        file_logging=False,
        strategy_type="row",
        components=[source, fanout, join, sink],
        metadata={},
    )

    execution = handler.execute_job(runtime_job)

    mh = handler.job_info.metrics_handler
    attempt = execution.attempts[0]
    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    sink_comp = get_component_by_name(runtime_job, "sink")
    sink_metrics = mh.get_comp_metrics(execution.id, attempt.id, sink_comp.id)
    assert sink_metrics.lines_received == source.count


class SlowSource(MultiSource):
    """Source that introduces a measurable delay between outputs."""

    async def process_row(self, payload, metrics):
        metrics.lines_received += 1
        yield Out("out", {"id": 0})
        await asyncio.sleep(0.05)
        metrics.lines_received += 1
        yield Out("out", {"id": 1})


def test_component_metrics_processing_time(schema_row_min):
    handler = JobExecutionHandler()
    source = SlowSource(
        name="slow",
        comp_type="slow_source",
        description="",
        routes={"out": [EdgeRef(to="sink", in_port="in")]},
        out_port_schemas={"out": schema_row_min},
    )
    sink = MultiEcho(
        name="sink",
        comp_type="multi_echo",
        description="",
        in_port_schemas={"in": schema_row_min},
        out_port_schemas={"out": schema_row_min},
        routes={"out": []},
    )

    runtime_job = RuntimeJob(
        name="SlowMetricsJob",
        num_of_retries=0,
        file_logging=False,
        strategy_type="row",
        components=[source, sink],
        metadata={},
    )

    execution = handler.execute_job(runtime_job)
    attempt = execution.attempts[0]
    metrics = handler.job_info.metrics_handler.get_comp_metrics(
        execution.id, attempt.id, source.id
    )
    assert metrics.processing_time.total_seconds() >= 0.05
