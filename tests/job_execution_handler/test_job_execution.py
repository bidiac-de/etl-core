from src.job_execution.job_execution_handler import JobExecutionHandler
from src.components.runtime_state import RuntimeState
import src.job_execution.job as job_module
from src.components.stubcomponents import StubComponent
from src.job_execution.job import Job
from datetime import datetime
from tests.helpers import get_component_by_name

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
        "metadata": {
            "created_by": 42,
            "created_at": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "test1",
                "comp_type": "test",
                "description": "a test comp",
            }
        ],
    }

    job = Job(**config)
    result = handler.execute_job(job)

    assert len(result.executions) == 1

    exec_record = result.executions[0]
    comp = get_component_by_name(job, "test1")
    comp_metrics = exec_record.attempts[0].component_metrics[comp.id]
    assert comp_metrics.status == RuntimeState.SUCCESS.value
    assert comp_metrics.lines_received == 1
    assert exec_record.job_metrics.status == RuntimeState.SUCCESS.value


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
        "metadata": {
            "created_by": 42,
            "created_at": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "comp1",
                "comp_type": "test",
                "description": "first",
                "next": ["comp2"],
            },
            {
                "name": "comp2",
                "comp_type": "test",
                "description": "second",
            },
        ],
    }

    job = Job(**config)
    result = handler.execute_job(job)

    # should complete successfully
    assert result.file_logging is True

    # single JobExecution entry
    assert len(result.executions) == 1
    exec_record = result.executions[0]
    assert exec_record.job_metrics.status == RuntimeState.SUCCESS.value

    # both components ran and metrics recorded
    metrics = exec_record.attempts[0].component_metrics
    comp1 = get_component_by_name(job, "comp1")
    comp2 = get_component_by_name(job, "comp2")
    assert set(metrics.keys()) == {comp1.id, comp2.id}
    assert metrics[comp1.id].lines_received == 1
    assert metrics[comp1.id].status == RuntimeState.SUCCESS.value
    assert metrics[comp2.id].lines_received == 1
    assert metrics[comp2.id].status == RuntimeState.SUCCESS.value


def test_execute_job_failing_and_cancelled_components():
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
            "created_by": 42,
            "created_at": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "comp1",
                "comp_type": "failtest",  # our failing component
                "description": "will fail",
                "next": ["comp2"],
            },
            {
                "name": "comp2",
                "comp_type": "test",
                "description": "should be cancelled",
            },
        ],
    }

    job = Job(**config)
    result = handler.execute_job(job)

    # Job-level assertions
    assert len(result.executions) == 1
    exec_record = result.executions[0]
    assert exec_record.job_metrics.status == RuntimeState.FAILED.value
    assert exec_record.attempts[0].error is not None
    assert ("fail stubcomponent failed") in exec_record.attempts[0].error

    # Component-level assertions
    comp1 = get_component_by_name(job, "comp1")
    comp2 = get_component_by_name(job, "comp2")
    comp1_metrics = exec_record.attempts[0].component_metrics[comp1.id]
    comp2_metrics = exec_record.attempts[0].component_metrics[comp2.id]
    assert (
        comp1_metrics.status == RuntimeState.FAILED.value
    ), "comp1 should have FAILED status"
    assert (
        comp2_metrics.status == RuntimeState.CANCELLED.value
    ), "comp2 should be CANCELLED due to dependency"


def test_retry_logic_and_metrics():
    handler = JobExecutionHandler()
    config = {
        "job_name": "RetryOnceJob",
        "num_of_retries": 1,
        "file_logging": False,
        "metadata": {
            "created_by": 42,
            "created_at": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "c1",
                "comp_type": "stub_fail_once",
                "description": "",
            }
        ],
    }
    job = Job(**config)
    result = handler.execute_job(job)

    # Should retry once, then succeed
    exec_record = result.executions[0]
    assert len(exec_record.attempts) == 2
    assert exec_record.job_metrics.status == RuntimeState.SUCCESS.value
    # lines_received comes from second execution
    comp = get_component_by_name(job, "c1")
    assert exec_record.attempts[1].component_metrics[comp.id].lines_received == 1


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
        "metadata": {
            "created_by": 42,
            "created_at": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "c1",
                "comp_type": "test",
                "description": "",
                "next": ["c2"],
            },
            {
                "name": "c2",
                "comp_type": "test",
                "description": "",
                "next": ["c3"],
            },
            {
                "name": "c3",
                "comp_type": "test",
                "description": "",
                "next": ["c4"],
            },
            {
                "name": "c4",
                "comp_type": "test",
                "description": "",
            },
        ],
    }

    job = Job(**config)
    # Allow up to 4 workers, but dependencies enforce sequential execution
    result = handler.execute_job(job)

    # Should be a single successful execution
    assert len(result.executions) == 1
    exec_record = result.executions[0]
    assert exec_record.job_metrics.status == RuntimeState.SUCCESS.value

    # Every component should have run once and completed
    metrics = exec_record.attempts[0].component_metrics
    comp1 = get_component_by_name(job, "c1")
    assert metrics[comp1.id].lines_received == 1
    assert metrics[comp1.id].status == RuntimeState.SUCCESS.value
    comp2 = get_component_by_name(job, "c2")
    assert metrics[comp2.id].lines_received == 1
    assert metrics[comp2.id].status == RuntimeState.SUCCESS.value
    comp3 = get_component_by_name(job, "c3")
    assert metrics[comp3.id].lines_received == 1
    assert metrics[comp3.id].status == RuntimeState.SUCCESS.value
    comp4 = get_component_by_name(job, "c4")
    assert metrics[comp4.id].lines_received == 1
    assert metrics[comp4.id].status == RuntimeState.SUCCESS.value
