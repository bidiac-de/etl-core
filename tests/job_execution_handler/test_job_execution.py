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

    execution = handler.execute_job(job)
    attempt = execution.attempts[0]
    assert len(execution.attempts) == 1
    mh = handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS
    comp = get_component_by_name(job, "test1")

    comp_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp.id)
    assert comp_metrics.status == RuntimeState.SUCCESS
    assert comp_metrics.lines_received == 1


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
    execution = handler.execute_job(job)
    attempt = execution.attempts[0]
    assert len(execution.attempts) == 1
    mh = handler.job_info.metrics_handler

    assert job.file_logging is True

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    # both components ran and metrics recorded
    comp1 = get_component_by_name(job, "comp1")
    comp2 = get_component_by_name(job, "comp2")

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
    execution = handler.execute_job(job)
    attempt = execution.attempts[0]
    assert len(execution.attempts) == 1
    mh = handler.job_info.metrics_handler

    # Job-level assertions
    assert mh.get_job_metrics(execution.id).status == RuntimeState.FAILED
    assert attempt.error is not None
    assert ("fail stubcomponent failed") in attempt.error

    # Component-level assertions
    comp1 = get_component_by_name(job, "comp1")
    comp2 = get_component_by_name(job, "comp2")
    comp1_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp1.id)
    comp2_metrics = mh.get_comp_metrics(execution.id, attempt.id, comp2.id)
    assert (
        comp1_metrics.status == RuntimeState.FAILED
    ), "comp1 should have FAILED status"
    assert (
        comp2_metrics.status == RuntimeState.CANCELLED
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
    execution = handler.execute_job(job)
    attempt = execution.attempts[1]
    assert len(execution.attempts) == 2
    mh = handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS
    # lines_received comes from second execution
    comp = get_component_by_name(job, "c1")
    assert mh.get_comp_metrics(execution.id, attempt.id, comp.id).lines_received == 1


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
    execution = handler.execute_job(job)
    attempt = execution.attempts[0]
    assert len(execution.attempts) == 1
    mh = handler.job_info.metrics_handler

    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS

    comp1 = get_component_by_name(job, "c1")
    comp2 = get_component_by_name(job, "c2")
    comp3 = get_component_by_name(job, "c3")
    comp4 = get_component_by_name(job, "c4")

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


def test_execute_linear_chain_with_retry_metrics():
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
            "created_by": 42,
            "created_at": datetime.now(),
        },
        "strategy_type": "row",
        "components": [
            {
                "name": "c1",
                "comp_type": "stub_fail_once",
                "description": "",
                "next": ["c2"],
            },
            {"name": "c2", "comp_type": "test", "description": "", "next": ["c3"]},
            {"name": "c3", "comp_type": "test", "description": ""},
        ],
    }
    job = Job(**config)
    execution = handler.execute_job(job)

    # should have retried exactly once
    assert len(execution.attempts) == 2
    mh = handler.job_info.metrics_handler

    # grab the two components
    comp1 = get_component_by_name(job, "c1")
    comp2 = get_component_by_name(job, "c2")
    comp3 = get_component_by_name(job, "c3")
    first = execution.attempts[0]
    second = execution.attempts[1]

    # first attempt metrics
    m1_first = mh.get_comp_metrics(execution.id, first.id, comp1.id)
    m2_first = mh.get_comp_metrics(execution.id, first.id, comp2.id)
    m3_first = mh.get_comp_metrics(execution.id, first.id, comp3.id)
    assert m1_first.status == RuntimeState.FAILED
    assert m1_first.lines_received == 0
    assert m2_first.status == RuntimeState.CANCELLED
    assert m2_first.lines_received == 0
    assert m3_first.status == RuntimeState.CANCELLED
    assert m3_first.lines_received == 0

    # second attempt metrics
    m1_second = mh.get_comp_metrics(execution.id, second.id, comp1.id)
    m2_second = mh.get_comp_metrics(execution.id, second.id, comp2.id)
    m3_second = mh.get_comp_metrics(execution.id, second.id, comp3.id)
    assert m1_second.status == RuntimeState.SUCCESS
    assert m1_second.lines_received == 1
    assert m2_second.status == RuntimeState.SUCCESS
    assert m2_second.lines_received == 1
    assert m3_second.status == RuntimeState.SUCCESS
    assert m3_second.lines_received == 1

    # final job-level status should be SUCCESS
    assert mh.get_job_metrics(execution.id).status == RuntimeState.SUCCESS
