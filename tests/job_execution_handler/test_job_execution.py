from src.job_execution.job_execution_handler import JobExecutionHandler
from src.job_execution.job import JobStatus
import src.job_execution.job as job_module
from src.components.stubcomponents import StubComponent
from src.components.base_component import RuntimeState

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
        "JobID": "exec_1",
        "JobName": "ExecuteTestComponent",
        "NumOfRetries": 0,
        "FileLogging": False,
        "components": [
            {
                "id": 1,
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

    job = handler.create_job(config, user_id=1)
    result = handler.execute_job(job, max_workers=1)

    assert len(result.executions) == 1

    exec_record = result.executions[0]
    comp_metrics = exec_record.component_metrics[1]
    assert comp_metrics.lines_received == 1
    assert exec_record.status == JobStatus.COMPLETED.value


def test_execute_job_chain_components_file_logging(caplog):
    """
    A job with two chained TestComponents should:
      - run to COMPLETED
      - record a single execution
      - record metrics for both components
      - exercise the file_logging branch
    """
    handler = JobExecutionHandler()
    config = {
        "JobID": "chain_1",
        "JobName": "ChainJob",
        "NumOfRetries": 0,
        "FileLogging": True,  # exercise the file_logging path
        "components": [
            {
                "id": 1,
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
                "id": 2,
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

    job = handler.create_job(config, user_id=1)
    result = handler.execute_job(job, max_workers=1)

    # should complete successfully
    assert result.file_logging is True

    # single JobExecution entry
    assert len(result.executions) == 1
    exec_record = result.executions[0]
    assert exec_record.status == JobStatus.COMPLETED.value

    # both components ran and metrics recorded
    metrics = exec_record.component_metrics
    assert set(metrics.keys()) == {1, 2}
    assert metrics[1].lines_received == 1
    assert metrics[2].lines_received == 1


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
        "JobID": "fail_chain",
        "JobName": "ChainErrorJob",
        "NumOfRetries": 0,
        "FileLogging": False,
        "components": [
            {
                "id": 1,
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
                "id": 2,
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

    job = handler.create_job(config, user_id=1)
    result = handler.execute_job(job, max_workers=1)

    # Job-level assertions
    assert len(result.executions) == 1
    exec_record = result.executions[0]
    assert exec_record.status == JobStatus.FAILED.value
    assert exec_record.error is not None
    assert (
        "One or more components failed due to dependency failure. "
        "Depending components were skipped"
    ) in exec_record.error

    # Component-level assertions
    comp1 = job.components[1]
    comp2 = job.components[2]
    assert comp1.status == RuntimeState.FAILED, "comp1 should have FAILED status"
    assert (
        comp2.status == RuntimeState.SKIPPED
    ), "comp2 should be SKIPPED due to dependency"


def test_retry_logic_and_metrics(tmp_path):
    handler = JobExecutionHandler()
    config = {
        "JobID": "retry_1",
        "JobName": "RetryOnceJob",
        "NumOfRetries": 1,
        "FileLogging": False,
        "components": [
            {
                "id": 1,
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
    job = handler.create_job(config, user_id=1)
    result = handler.execute_job(job, max_workers=1)

    # Should retry once, then succeed
    exec_record = result.executions[0]
    assert exec_record.status == JobStatus.COMPLETED.value
    # lines_received comes from second execution
    assert exec_record.component_metrics[1].lines_received == 2
