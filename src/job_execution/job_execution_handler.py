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
        self._local = threading.local()

    def execute_job(self, job: Job, max_workers: int = 4) -> Job:
        execution = JobExecution(job=job)
        self.job_information_handler.logging_handler.update_job_name(job.name)
        execution.file_logger = self.job_information_handler.logging_handler.logger
        job.executions.append(execution)
        self._local.execution = execution
        execution.set_started()
        logger.info("Starting execution of job '%s'", job.name)

        total_attempts = job.num_of_retries + 1
        for attempt_index in range(total_attempts):
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
                "Attempt %d for job '%s'",
                attempt_index + 1,
                job.name,
            )
            try:
                self._run_attempt(
                    job,
                    max_workers,
                )
                # Success: no exception, stop retrying
                execution.status = RuntimeState.SUCCESS.value
                return job
            except Exception as exc:
                logger.warning("Attempt %d failed: %s", attempt_index + 1, exc)
                self._local.execution.file_logger.warning(
                    "Attempt %d failed: %s", attempt_index + 1, exc
                )
                # If this was the last allowed attempt, finalize as failure
                if attempt_index == job.num_of_retries:
                    self._finalize_failure(exc)
                    return job

        return job

    def _run_attempt(
        self,
        job: Job,
        max_workers: int,
    ) -> None:
        # initialize tracking sets on attempt, reset each retry
        self._local.attempt.pending = set(job.components.keys())

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # submit initial ready components (roots)
            futures: dict[concurrent.futures.Future, Component] = {}
            for comp in job.root_components:
                self._local.execution.file_logger.debug("Submitting '%s'", comp.name)
                metrics = self._local.attempt.component_metrics[comp.id]
                fut = executor.submit(
                    self._execute_component,
                    comp,
                    None,
                    metrics,
                )
                futures[fut] = comp
                self._local.attempt.pending.discard(comp.id)

            while futures:
                done, _ = concurrent.futures.wait(
                    futures,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for fut in done:
                    comp = futures.pop(fut)
                    self._handle_future(
                        fut,
                        comp,
                    )
                    self._schedule_next(
                        comp,
                        executor,
                        futures,
                    )

        self._mark_unrunnable()
        self._finalize_success(job)

    def _handle_future(
        self,
        fut: concurrent.futures.Future,
        comp: Component,
    ) -> None:
        try:
            fut.result()
            self._local.attempt.succeeded.add(comp.id)
            self._update_job_metrics()
        except Exception:
            self._local.attempt.component_metrics[comp.id].status = RuntimeState.FAILED
            self._local.attempt.failed.add(comp.id)
            self._local.execution.file_logger.error(
                "Component '%s' FAILED", comp.name, exc_info=True
            )

    def _update_job_metrics(
        self,
    ) -> None:
        try:
            total = sum(
                m.lines_received for m in self._local.attempt.component_metrics.values()
            )
            elapsed = (
                datetime.datetime.now() - self._local.execution.job_metrics.started_at
            )
            self._local.attempt.processing_time = elapsed
            self._local.execution.job_metrics.calc_throughput(total)
        except Exception:
            pass

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
        duration = datetime.datetime.now() - self._local.execution.started_at
        self._local.execution.job_metrics.processing_time = duration
        self._local.execution.job_metrics.status = RuntimeState.SUCCESS.value
        self.job_information_handler.metrics_handler.add_job_metrics(
            job.id, self._local.execution.job_metrics
        )
        for cid, comp in job.components.items():
            self.job_information_handler.metrics_handler.add_component_metrics(
                job.id, cid, self._local.attempt.component_metrics[cid]
            )
        self._local.execution.status = RuntimeState.SUCCESS.value

    def _finalize_failure(
        self,
        exc: Exception,
    ) -> None:
        self._local.execution.status = RuntimeState.FAILED.value
        self._local.execution.job_metrics.status = RuntimeState.FAILED.value
        self._local.execution.error = str(exc)

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
