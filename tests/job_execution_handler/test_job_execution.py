from datetime import datetime
import asyncio
import threading
from typing import ClassVar

import pytest
from sqlmodel import SQLModel, create_engine
from sqlmodel.pool import StaticPool

import etl_core.job_execution.runtimejob as runtimejob_module
from etl_core.components.runtime_state import RuntimeState
from etl_core.components.stubcomponents import StubComponent, MultiSource
from etl_core.components.component_registry import register_component
from etl_core.context.environment import Environment
from etl_core.job_execution.job_execution_handler import (
    JobExecutionHandler,
    ExecutionAlreadyRunning,
)
from etl_core.persistence.handlers.execution_records_handler import (
    ExecutionRecordsHandler,
)
from tests.helpers import get_component_by_name, runtime_job_from_config


# ensure Job._build_components() can find TestComponent
runtimejob_module.TestComponent = StubComponent


def _handler_with_in_memory_records() -> (
    tuple[JobExecutionHandler, ExecutionRecordsHandler]
):
    """Provision a handler whose persistence writes to an isolated in-memory DB."""
    mem_engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(mem_engine)

    handler = JobExecutionHandler()
    records = ExecutionRecordsHandler(engine_=mem_engine)
    handler._exec_records_handler = records  # type: ignore[attr-defined]
    return handler, records


@register_component("blocking_stub", hidden=True)
class BlockingStubComponent(StubComponent):
    """
    Component that blocks until its release event is set,
      to test concurrency guards.
    """

    start_event: ClassVar[threading.Event] = threading.Event()
    release_event: ClassVar[threading.Event] = threading.Event()

    @classmethod
    def reset_events(cls) -> None:
        cls.start_event = threading.Event()
        cls.release_event = threading.Event()

    async def process_row(self, row, metrics):  # type: ignore[override]
        loop = asyncio.get_running_loop()
        self.__class__.start_event.set()
        await loop.run_in_executor(None, self.__class__.release_event.wait)

        async for item in super().process_row(row, metrics):
            yield item


@register_component("prepare_stub", hidden=True)
class PrepareAwareComponent(StubComponent):
    """Component that records environments passed to prepare_for_execution."""

    prepared_with: ClassVar[list[Environment | None]] = []

    @classmethod
    def reset(cls) -> None:
        cls.prepared_with = []

    def prepare_for_execution(self, environment: Environment | None) -> None:
        self.__class__.prepared_with.append(environment)


@register_component("exploding_source", hidden=True)
class ExplodingSource(MultiSource):
    """Source component raises after short suspension to trigger TaskGroup aggregation."""

    fail_message: str = "boom"

    async def process_row(self, payload, metrics):  # type: ignore[override]
        if False:  # pragma: no cover - ensure async generator semantics
            yield None
        await asyncio.sleep(0)
        raise RuntimeError(self.fail_message)


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
                "metadata": {
                    "user_id": 42,
                    "timestamp": datetime.now(),
                },
            },
        ],
    }

    runtime_job = runtime_job_from_config(config)

    log_handler = handler.job_info.logging_handler
    base_log_dir = log_handler.base_log_dir
    before_logs = set(base_log_dir.rglob("*.log")) if base_log_dir.exists() else set()

    execution = handler.execute_job(runtime_job)
    attempt = execution.attempts[0]
    assert len(execution.attempts) == 1
    mh = handler.job_info.metrics_handler

    assert runtime_job.file_logging is True
    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    job_log_dir = base_log_dir / log_handler.job_name
    assert job_log_dir.exists(), f"expected log directory {job_log_dir} to exist"

    after_logs = set(base_log_dir.rglob("*.log"))
    new_logs = {path for path in after_logs - before_logs if path.parent == job_log_dir}
    assert new_logs, "expected job execution to create a log file"
    assert any(path.stat().st_size > 0 for path in new_logs)

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


def test_execute_job_multiple_failures_coalesces_exception_group(schema_row_min):
    """When multiple components fail, handler exposes the underlying error string."""

    handler = JobExecutionHandler()
    config = {
        "name": "DualFailJob",
        "num_of_retries": 0,
        "file_logging": False,
        "metadata": {
            "user_id": 13,
            "timestamp": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "fail_a",
                "comp_type": "exploding_source",
                "description": "raises first",
                "routes": {"out": []},
                "out_port_schemas": {"out": schema_row_min},
                "fail_message": "boom-first",
                "metadata": {
                    "user_id": 13,
                    "timestamp": datetime.now(),
                },
            },
            {
                "name": "fail_b",
                "comp_type": "exploding_source",
                "description": "raises second",
                "routes": {"out": []},
                "out_port_schemas": {"out": schema_row_min},
                "fail_message": "boom-second",
                "metadata": {
                    "user_id": 13,
                    "timestamp": datetime.now(),
                },
            },
        ],
    }

    runtime_job = runtime_job_from_config(config)
    execution = handler.execute_job(runtime_job)
    attempt = execution.attempts[0]
    mh = handler.job_info.metrics_handler

    assert len(execution.attempts) == 1
    assert mh.get_job_metrics(execution.id).status == RuntimeState.FAILED
    assert attempt.error in {"boom-first", "boom-second"}
    assert "ExceptionGroup" not in attempt.error

    comp_a = get_component_by_name(runtime_job, "fail_a")
    comp_b = get_component_by_name(runtime_job, "fail_b")

    status_a = mh.get_comp_metrics(execution.id, attempt.id, comp_a.id).status
    status_b = mh.get_comp_metrics(execution.id, attempt.id, comp_b.id).status

    assert status_a == RuntimeState.FAILED
    assert status_b in {RuntimeState.FAILED, RuntimeState.CANCELLED}


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


def test_execute_job_rejects_overlapping_runs(schema_row_min):
    handler, records = _handler_with_in_memory_records()
    BlockingStubComponent.reset_events()

    config = {
        "name": "ConcurrencyGuardJob",
        "num_of_retries": 0,
        "file_logging": False,
        "metadata": {"user_id": 1, "timestamp": datetime.now()},
        "strategy_type": "row",
        "components": [
            {
                "name": "blocker",
                "comp_type": "blocking_stub",
                "description": "blocks until released",
                "routes": {"out": []},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {"user_id": 1, "timestamp": datetime.now()},
            }
        ],
    }

    runtime_job = runtime_job_from_config(config)

    result: dict[str, object] = {}

    def _run_job() -> None:
        result["execution"] = handler.execute_job(runtime_job)

    worker = threading.Thread(target=_run_job, daemon=True)
    worker.start()

    assert BlockingStubComponent.start_event.wait(timeout=5), "worker did not start"

    with pytest.raises(ExecutionAlreadyRunning):
        handler.execute_job(runtime_job)

    BlockingStubComponent.release_event.set()
    worker.join(timeout=5)
    assert not worker.is_alive(), "worker thread did not finish"

    execution = result.get("execution")
    assert execution is not None, "execution result missing"

    persisted_exec, attempts = records.get_execution(execution.id)
    assert persisted_exec is not None
    assert persisted_exec.status == "SUCCESS"
    assert len(attempts) == 1
    assert attempts[0].status == "SUCCESS"

    BlockingStubComponent.reset_events()


def test_execute_job_invokes_prepare_for_execution(schema_row_min):
    handler, _ = _handler_with_in_memory_records()
    PrepareAwareComponent.reset()

    config = {
        "name": "PrepareHookJob",
        "num_of_retries": 0,
        "file_logging": False,
        "metadata": {"user_id": 7, "timestamp": datetime.now()},
        "strategy_type": "row",
        "components": [
            {
                "name": "prep",
                "comp_type": "prepare_stub",
                "description": "records preparation environment",
                "routes": {"out": []},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {"user_id": 7, "timestamp": datetime.now()},
            }
        ],
    }

    runtime_job = runtime_job_from_config(config)
    handler.execute_job(runtime_job, environment=Environment.TEST)

    assert PrepareAwareComponent.prepared_with == [Environment.TEST]
    PrepareAwareComponent.reset()


def test_execute_job_persists_attempt_sequence(schema_row_min):
    handler, records = _handler_with_in_memory_records()

    config = {
        "name": "RetryPersistenceJob",
        "num_of_retries": 1,
        "file_logging": False,
        "metadata": {"user_id": 11, "timestamp": datetime.now()},
        "strategy_type": "row",
        "components": [
            {
                "name": "flaky",
                "comp_type": "stub_fail_once",
                "description": "fails first attempt then succeeds",
                "routes": {"out": []},
                "in_port_schemas": {"in": schema_row_min},
                "out_port_schemas": {"out": schema_row_min},
                "metadata": {"user_id": 11, "timestamp": datetime.now()},
            }
        ],
    }

    runtime_job = runtime_job_from_config(config)
    execution = handler.execute_job(runtime_job)

    persisted_exec, attempts = records.get_execution(execution.id)
    assert persisted_exec is not None
    assert persisted_exec.status == "SUCCESS"
    assert len(attempts) == 2
    assert [a.status for a in attempts] == ["FAILED", "SUCCESS"]
    assert attempts[0].error is not None
    assert attempts[1].error is None
