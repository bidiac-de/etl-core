import datetime
import logging
import concurrent.futures
from typing import Any

from src.job_execution.job import Job, ExecutionAttempt, JobExecution
from src.components.base_component import Component
from src.components.runtime_state import RuntimeState
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.job_execution.job_information_handler import JobInformationHandler
from src.metrics.system_metrics import SystemMetricsHandler

logger = logging.getLogger("job.ExecutionHandler")


class JobExecutionHandler:
    """
    Handles the execution of ETL jobs by managing component execution
    Preserves command pattern while supporting parallel execution
    """

    def __init__(self):
        """
        Initialize the JobExecutionHandler with the component registry and logging
        """
        self.job_information_handler = JobInformationHandler(job_name="no_job_assigned")
        self.system_metrics_handler = SystemMetricsHandler()
        self.running_executions: list[JobExecution] = []

    def execute_job(self, job: Job, max_workers: int = 4) -> Job:
        if any(exec_.job == job for exec_ in self.running_executions):
            logger.warning(
                "Job '%s' is already running; skipping new execution", job.name
            )
            return job

        execution = JobExecution(
            job=job,
            number_of_attempts=job.num_of_retries + 1,
            handler=self,
        )

        for attempt_index in range(execution.number_of_attempts):
            attempt = execution.create_attempt()

            execution.file_logger.debug(
                "Attempt %d for job '%s'", attempt_index + 1, job.name
            )
            try:
                attempt.run_attempt(max_workers)
                execution.job_metrics.status = RuntimeState.SUCCESS.value
                return job
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("Attempt %d failed: %s", attempt_index + 1, exc)
                execution.file_logger.warning(
                    "Attempt %d failed: %s", attempt_index + 1, exc
                )
                if attempt_index == execution.number_of_attempts - 1:
                    self.finalize_failure(exc, execution, attempt)
                    return job

        return job

    def handle_future(
        self,
        fut: concurrent.futures.Future,
        comp: Component,
        attempt: ExecutionAttempt,
        execution: JobExecution,
    ) -> None:
        try:
            fut.result()
            attempt.succeeded.add(comp.id)
            execution.job_metrics.update_metrics(attempt.component_metrics)
        except Exception:
            attempt.component_metrics[comp.id].status = RuntimeState.FAILED
            attempt.failed.add(comp.id)
            execution.file_logger.error(
                "Component '%s' FAILED", comp.name, exc_info=True
            )

    def schedule_next(
        self,
        comp: Component,
        executor: concurrent.futures.Executor,
        futures: dict[concurrent.futures.Future, Component],
        attempt: ExecutionAttempt,
        execution: JobExecution,
    ) -> None:
        for nxt in comp.next_components:
            if nxt.id in attempt.succeeded | attempt.failed | attempt.cancelled:
                continue

            prev_ids = {p.id for p in nxt.prev_components}
            metrics = attempt.component_metrics[nxt.id]

            if prev_ids.issubset(attempt.succeeded):
                execution.file_logger.debug("Submitting '%s'", nxt.name)
                fut = executor.submit(self.execute_component, nxt, None, metrics)
                futures[fut] = nxt
                attempt.pending.discard(nxt.id)
            elif prev_ids & (attempt.failed | attempt.cancelled):
                metrics.status = RuntimeState.CANCELLED
                attempt.cancelled.add(nxt.id)
                attempt.pending.discard(nxt.id)
                execution.file_logger.warning("Component '%s' CANCELLED", nxt.name)

    def mark_unrunnable(
        self,
        attempt: ExecutionAttempt,
        execution: JobExecution,
    ) -> None:
        for pid in list(attempt.pending):
            comp = execution.job.components[pid]
            metrics = attempt.component_metrics[pid]
            metrics.status = RuntimeState.CANCELLED
            attempt.cancelled.add(pid)
            execution.file_logger.warning(
                "Component '%s' CANCELLED (no runnable path)", comp.name
            )

    def finalize_success(
        self,
        execution: JobExecution,
        attempt: ExecutionAttempt,
    ) -> None:
        if attempt.failed:
            raise RuntimeError(
                "One or more components failed; dependent components cancelled"
            )

        duration = datetime.datetime.now() - execution.job_metrics.started_at
        execution.job_metrics.processing_time = duration
        execution.job_metrics.status = RuntimeState.SUCCESS.value

        self.job_information_handler.metrics_handler.add_job_metrics(
            execution.job.id, execution.job_metrics
        )

        for cid in execution.job.components:
            self.job_information_handler.metrics_handler.add_component_metrics(
                execution.job.id,
                cid,
                attempt.component_metrics[cid],
            )

        self.running_executions.remove(execution)

    def finalize_failure(
        self,
        exc: Exception,
        execution: JobExecution,
        attempt: ExecutionAttempt,
    ) -> None:
        execution.job_metrics.status = RuntimeState.FAILED.value
        attempt.error = str(exc)
        self.running_executions.remove(execution)

    def execute_component(
        self,
        component: Component,
        data: Any,
        metrics: ComponentMetrics,
    ) -> Any:
        """
        Execute a single component
        !!note: will only work when component is a concrete class !!
        """
        try:
            logger.info("Executing component: %s", component.name)
            metrics.status = RuntimeState.RUNNING
            metrics.set_started()
            result = component.execute(data, metrics)
            metrics.status = RuntimeState.SUCCESS
            logger.info("Component %s completed successfully", component.name)
            return result

        except Exception as e:
            logger.exception("Component %s failed: %s", component.name, e)
            metrics.status = RuntimeState.FAILED
            raise
