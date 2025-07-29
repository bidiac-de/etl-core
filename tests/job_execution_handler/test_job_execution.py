from src.job_execution.job_execution_handler import JobExecutionHandler
from src.components.runtime_state import RuntimeState
from tests.helpers import get_by_temp_id
import src.job_execution.job as job_module
from src.components.stubcomponents import StubComponent
from src.job_execution.job import Job
from datetime import datetime

# ensure Job._build_components() can find TestComponent
job_module.TestComponent = StubComponent


def test_execute_job_single_test_component():
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
        "created_by": 42,
        "created_at": datetime.now(),
        "component_configs": [
            {
                "temp_id": 1,
                "name": "test1",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "a test comp",
                "x_coord": 0.0,
                "y_coord": 0.0,
                "created_by": 1,
                "created_at": "2024-01-01T00:00:00",
            }
        ],
    }

    job = Job(**config)
    result = handler.execute_job(job, max_workers=1)

    assert len(result.executions) == 1

    exec_record = result.executions[0]
    comp = get_by_temp_id(job.components, 1)
    comp_metrics = exec_record.component_metrics[comp.id]
    assert comp_metrics.lines_received == 1
    assert comp_metrics.status == RuntimeState.SUCCESS
    assert exec_record.status == RuntimeState.SUCCESS.value


def test_execute_job_chain_components_file_logging():
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
        "file_logging": True,  # exercise the file_logging path
        "created_by": 42,
        "created_at": datetime.now(),
        "component_configs": [
            {
                "temp_id": 1,
                "name": "comp1",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "first",
                "x_coord": 0.0,
                "y_coord": 0.0,
                "created_by": 1,
                "created_at": "2024-01-01T00:00:00",
                "next": [2],
            },
            {
                "temp_id": 2,
                "name": "comp2",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "second",
                "x_coord": 0.0,
                "y_coord": 0.0,
                "created_by": 1,
                "created_at": "2024-01-01T00:00:00",
            },
        ],
    }

    job = Job(**config)
    result = handler.execute_job(job, max_workers=1)

    # should complete successfully
    assert result.file_logging is True

    # single JobExecution entry
    assert len(result.executions) == 1
    exec_record = result.executions[0]
    assert exec_record.status == RuntimeState.SUCCESS.value

    # both components ran and metrics recorded
    metrics = exec_record.component_metrics
    comp1 = get_by_temp_id(job.components, 1)
    comp2 = get_by_temp_id(job.components, 2)
    assert set(metrics.keys()) == {comp1.id, comp2.id}
    assert metrics[comp1.id].lines_received == 1
    assert metrics[comp1.id].status == RuntimeState.SUCCESS
    assert metrics[comp2.id].lines_received == 1
    assert metrics[comp2.id].status == RuntimeState.SUCCESS


def test_execute_job_failing_and_skipped_components():
    """
    A job with a failing first component and a dependent second component should:
      - end up FAILED
      - set job.error appropriately
      - record no executions
      - mark first component FAILED
      - mark second component SKIPPED
    """

    handler = JobExecutionHandler()
    config = {
        "job_name": "ChainErrorJob",
        "num_of_retries": 0,
        "file_logging": False,
        "created_by": 42,
        "created_at": datetime.now(),
        "component_configs": [
            {
                "temp_id": 1,
                "name": "comp1",
                "comp_type": "failtest",  # our failing component
                "strategy_type": "row",
                "description": "will fail",
                "x_coord": 0.0,
                "y_coord": 0.0,
                "created_by": 1,
                "created_at": "2024-01-01T00:00:00",
                "next": [2],
            },
            {
                "temp_id": 2,
                "name": "comp2",
                "comp_type": "test",  # normal TestComponent
                "strategy_type": "row",
                "description": "should be skipped",
                "x_coord": 1.0,
                "y_coord": 1.0,
                "created_by": 1,
                "created_at": "2024-01-01T00:00:00",
            },
        ],
    }

    job = Job(**config)
    result = handler.execute_job(job, max_workers=1)

    # Job-level assertions
    assert len(result.executions) == 1
    exec_record = result.executions[0]
    assert exec_record.status == RuntimeState.FAILED.value
    assert exec_record.error is not None
    assert (
        "One or more components failed; dependent components cancelled"
    ) in exec_record.error

    # Component-level assertions
    comp1 = get_by_temp_id(job.components, 1)
    comp2 = get_by_temp_id(job.components, 2)
    comp1_metrics = exec_record.component_metrics[comp1.id]
    comp2_metrics = exec_record.component_metrics[comp2.id]
    assert (
        comp1_metrics.status == RuntimeState.FAILED
    ), "comp1 should have FAILED status"
    assert (
        comp2_metrics.status == RuntimeState.CANCELLED
    ), "comp2 should be SKIPPED due to dependency"


def test_retry_logic_and_metrics():
    handler = JobExecutionHandler()
    config = {
        "job_name": "RetryOnceJob",
        "num_of_retries": 1,
        "file_logging": False,
        "created_by": 42,
        "created_at": datetime.now(),
        "component_configs": [
            {
                "temp_id": 1,
                "name": "c1",
                "comp_type": "stub_fail_once",
                "strategy_type": "row",
                "description": "",
                "x_coord": 0.0,
                "y_coord": 0.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
            }
        ],
    }
    job = Job(**config)
    result = handler.execute_job(job, max_workers=1)

    # Should retry once, then succeed
    exec_record = result.executions[0]
    assert exec_record.status == RuntimeState.SUCCESS.value
    # lines_received comes from second execution
    comp = get_by_temp_id(job.components, 1)
    assert exec_record.component_metrics[comp.id].lines_received == 1


def test_execute_job_linear_chain():
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
        "created_by": 42,
        "created_at": datetime.now(),
        "component_configs": [
            {
                "temp_id": 1,
                "name": "c1",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "x_coord": 0.0,
                "y_coord": 0.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
                "next": [2],
            },
            {
                "temp_id": 2,
                "name": "c2",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "x_coord": 1.0,
                "y_coord": 0.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
                "next": [3],
            },
            {
                "temp_id": 3,
                "name": "c3",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "x_coord": 2.0,
                "y_coord": 0.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
                "next": [4],
            },
            {
                "temp_id": 4,
                "name": "c4",
                "comp_type": "test",
                "strategy_type": "row",
                "description": "",
                "x_coord": 3.0,
                "y_coord": 0.0,
                "created_by": 1,
                "created_at": "2025-01-01T00:00:00",
            },
        ],
    }

    job = Job(**config)
    # Allow up to 4 workers, but dependencies enforce sequential execution
    result = handler.execute_job(job, max_workers=4)

    # Should be a single successful execution
    assert len(result.executions) == 1
    exec_record = result.executions[0]
    assert exec_record.status == RuntimeState.SUCCESS.value

    # Every component should have run once and completed
    metrics = exec_record.component_metrics
    for tid in range(1, 5):
        comp = get_by_temp_id(job.components, tid)
        assert metrics[comp.id].lines_received == 1
        assert metrics[comp.id].status == RuntimeState.SUCCESS
