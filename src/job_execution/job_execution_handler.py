import datetime
import logging
import concurrent.futures
from typing import Any
import threading

from src.job_execution.job import Job, ExecutionAttempt
from src.components.base_component import Component
from src.components.runtime_state import RuntimeState
from src.job_execution.job import JobExecution
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.job_execution.job_information_handler import JobInformationHandler
from src.metrics.system_metrics import SystemMetricsHandler
from src.metrics.metrics_registry import get_metrics_class

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
        self._local = threading.local()

    def execute_job(self, job: Job, max_workers: int = 4) -> Job:
        if any(exec_.job == job for exec_ in self.running_executions):
            logger.warning(
                "Job '%s' is already running; skipping new execution", job.name
            )
            return job

        execution = JobExecution(job=job, number_of_attempts=job.num_of_retries + 1)
        self.running_executions.append(execution)
        self.job_information_handler.logging_handler.update_job_name(job.name)
        execution.file_logger = self.job_information_handler.logging_handler.logger
        job.executions.append(execution)
        self._local.execution = execution
        logger.info("Starting execution of job '%s'", job.name)

        for attempt_index in range(execution.number_of_attempts):
            attempt = ExecutionAttempt(attempt_number=attempt_index + 1)
            execution.attempts.append(attempt)
            self._local.attempt = attempt
            components = job.components

            # initialize metrics for each component
            for comp in components.values():
                MetricsCls = get_metrics_class(comp.comp_type)
                metrics = MetricsCls(
                    processing_time=datetime.timedelta(0),
                    error_count=0,
                )
                attempt.component_metrics[comp.id] = metrics

            execution.file_logger.debug(
                "Attempt %d for job '%s'", attempt_index + 1, job.name
            )
            try:
                attempt.run_attempt(job, self, max_workers)
                execution.job_metrics.status = RuntimeState.SUCCESS.value
                return job
            except Exception as exc:
                logger.warning("Attempt %d failed: %s", attempt_index + 1, exc)
                execution.file_logger.warning(
                    "Attempt %d failed: %s", attempt_index + 1, exc
                )
                if attempt_index == execution.number_of_attempts - 1:
                    # Final attempt failed, finalize the job execution
                    self._finalize_failure(exc)
                    return job

        return job

    def _handle_future(
        self,
        fut: concurrent.futures.Future,
        comp: Component,
    ) -> None:
        try:
            fut.result()
            self._local.attempt.succeeded.add(comp.id)
            self._local.execution.job_metrics.update_metrics(
                self._local.attempt.component_metrics
            )
        except Exception:
            self._local.attempt.component_metrics[comp.id].status = RuntimeState.FAILED
            self._local.attempt.failed.add(comp.id)
            self._local.execution.file_logger.error(
                "Component '%s' FAILED", comp.name, exc_info=True
            )

    def _schedule_next(
        self,
        comp: Component,
        executor: concurrent.futures.ThreadPoolExecutor,
        futures: dict[concurrent.futures.Future, Component],
    ) -> None:
        for nxt in comp.next_components:
            if (
                nxt.id
                in self._local.attempt.succeeded
                | self._local.attempt.failed
                | self._local.attempt.cancelled
            ):
                continue
            prev_ids = {p.id for p in nxt.prev_components}
            metrics = self._local.attempt.component_metrics[nxt.id]
            # If all previous components succeeded, schedule the next component
            if prev_ids.issubset(self._local.attempt.succeeded):
                self._local.execution.file_logger.debug("Submitting '%s'", nxt.name)
                fut = executor.submit(self._execute_component, nxt, None, metrics)
                futures[fut] = nxt
                self._local.attempt.pending.discard(nxt.id)
            # If any previous component failed or was cancelled, mark this as cancelled
            elif prev_ids & (
                self._local.attempt.failed | self._local.attempt.cancelled
            ):
                metrics.status = RuntimeState.CANCELLED
                self._local.attempt.cancelled.add(nxt.id)
                self._local.attempt.pending.discard(nxt.id)
                self._local.execution.file_logger.warning(
                    "Component '%s' CANCELLED", nxt.name
                )

    def _mark_unrunnable(self) -> None:
        # Mark all pending components as cancelled if they have no way to run
        for pid in list(self._local.attempt.pending):
            comp = self._local.execution.job.components[pid]
            metrics = self._local.attempt.component_metrics[pid]
            metrics.status = RuntimeState.CANCELLED
            self._local.attempt.cancelled.add(pid)
            self._local.execution.file_logger.warning(
                "Component '%s' CANCELLED (no runnable path)", comp.name
            )

    def _finalize_success(self, job: Job) -> None:
        if self._local.attempt.failed:
            raise RuntimeError(
                "One or more components failed; dependent components cancelled"
            )
        duration = (
            datetime.datetime.now() - self._local.execution.job_metrics.started_at
        )
        self._local.execution.job_metrics.processing_time = duration
        self._local.execution.job_metrics.status = RuntimeState.SUCCESS.value
        self.job_information_handler.metrics_handler.add_job_metrics(
            job.id, self._local.execution.job_metrics
        )
        for cid, comp in job.components.items():
            self.job_information_handler.metrics_handler.add_component_metrics(
                job.id, cid, self._local.attempt.component_metrics[cid]
            )
        self.running_executions.remove(self._local.execution)

    def _finalize_failure(
        self,
        exc: Exception,
    ) -> None:
        self._local.execution.job_metrics.status = RuntimeState.FAILED.value
        self._local.attempt.error = str(exc)
        self.running_executions.remove(self._local.execution)

    def _execute_component(
        self, component: Component, data: Any, metrics: ComponentMetrics
    ) -> Any:
        """
        Execute a single component
        !!note: will only work when component is a concrete class !!
        :param component: The component to execute
        :param data: Input data for the component
        :return: Result of the component execution
        """
        try:
            logger.info(f"Executing component: {component.name}")
            metrics.status = RuntimeState.RUNNING

            metrics.set_started()
            result = component.execute(data, metrics)

            metrics.status = RuntimeState.SUCCESS
            logger.info(f"Component {component.name} completed successfully")
            return result

        except Exception as e:
            logger.exception(f"Component {component.name} failed: {str(e)}")
            metrics.status = RuntimeState.FAILED
            raise
