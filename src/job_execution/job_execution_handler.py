import datetime
import logging
import concurrent.futures
from typing import Any

from src.job_execution.job import Job
from src.components.base_component import Component, RuntimeState
from src.job_execution.job import JobExecution
from src.metrics.job_metrics import JobMetrics
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

    def execute_job(self, job: Job, max_workers: int = 4) -> Job:
        logger.info("Starting execution of job '%s'", job.name)
        self.job_information_handler.logging_handler.update_job_name(job.name)
        file_logger = self.job_information_handler.logging_handler.logger

        retry = 0
        execution = self._init_job_execution(job)

        while True:
            try:
                file_logger.debug("Attempt %d for job '%s'", retry + 1, job.name)
                self._run_attempt(job, execution, max_workers, file_logger)
                return job
            except Exception as exc:
                retry += 1
                execution.job_metrics.error_count = retry
                logger.warning("Attempt %d failed: %s", retry, exc)
                file_logger.warning("Attempt %d failed: %s", retry, exc)
                if retry > job.num_of_retries:
                    self._finalize_failure(job, execution, exc, file_logger)
                    return job

    def _init_job_execution(self, job: Job) -> JobExecution:
        exec_obj = JobExecution(job=job, status=RuntimeState.RUNNING.value)
        job.executions.append(exec_obj)
        start = datetime.datetime.now()
        exec_obj.started_at = start
        exec_obj.job_metrics = JobMetrics(
            started_at=start, processing_time=0, error_count=0
        )
        exec_obj.component_metrics = {}
        return exec_obj

    def _run_attempt(
        self,
        job: Job,
        execution: JobExecution,
        max_workers: int,
        file_logger: logging.Logger,
    ) -> None:
        components = job.components
        pending = set(components.keys())
        succeeded, failed, cancelled = set(), set(), set()

        roots = [c for c in components.values() if not c.prev_components]
        file_logger.debug("Root components: %s", [c.name for c in roots])

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = self._submit_roots(roots, executor, pending, file_logger)

            while futures:
                done, _ = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )
                for fut in done:
                    comp = futures.pop(fut)
                    self._handle_future(
                        fut,
                        comp,
                        execution,
                        succeeded,
                        failed,
                        cancelled,
                        pending,
                        file_logger,
                    )
                    self._schedule_next(
                        comp,
                        executor,
                        futures,
                        succeeded,
                        failed,
                        cancelled,
                        pending,
                        file_logger,
                    )

        self._mark_unrunnable(components, pending, cancelled, file_logger)
        self._finalize_success(job, execution, succeeded, failed, cancelled)

    def _submit_roots(
        self,
        roots: list[Component],
        executor: concurrent.futures.ThreadPoolExecutor,
        pending: set,
        file_logger: logging.Logger,
    ) -> dict[concurrent.futures.Future, Component]:
        futures = {}
        for comp in roots:
            file_logger.debug("Submitting root '%s'", comp.name)
            fut = executor.submit(self._execute_component, comp, None)
            futures[fut] = comp
            pending.discard(comp.id)
        return futures

    def _handle_future(
        self,
        fut: concurrent.futures.Future,
        comp: Component,
        execution: JobExecution,
        succeeded: set,
        failed: set,
        cancelled: set,
        pending: set,
        file_logger: logging.Logger,
    ) -> None:
        try:
            fut.result()
            comp.status = RuntimeState.SUCCESS
            succeeded.add(comp.id)
            self._update_metrics(comp, execution)
        except Exception:
            comp.status = RuntimeState.FAILED
            failed.add(comp.id)
            file_logger.error("Component '%s' FAILED", comp.name, exc_info=True)

    def _update_metrics(
        self,
        comp: Component,
        execution: JobExecution,
    ) -> None:
        try:
            metrics = comp.metrics
            execution.component_metrics[comp.id] = metrics
            total = sum(m.lines_received for m in execution.component_metrics.values())
            elapsed = datetime.datetime.now() - execution.job_metrics.started_at
            execution.processing_time = elapsed
            execution.job_metrics.calc_throughput(total)
        except Exception:
            pass

    def _schedule_next(
        self,
        comp: Component,
        executor: concurrent.futures.ThreadPoolExecutor,
        futures: dict[concurrent.futures.Future, Component],
        succeeded: set,
        failed: set,
        cancelled: set,
        pending: set,
        file_logger: logging.Logger,
    ) -> None:
        for nxt in comp.next_components:
            if nxt.id in succeeded | failed | cancelled:
                continue
            prev_ids = {p.id for p in nxt.prev_components}
            if prev_ids.issubset(succeeded):
                file_logger.debug("Submitting '%s'", nxt.name)
                fut = executor.submit(self._execute_component, nxt, None)
                futures[fut] = nxt
                pending.discard(nxt.id)
            elif prev_ids & (failed | cancelled):
                nxt.status = RuntimeState.CANCELLED
                cancelled.add(nxt.id)
                pending.discard(nxt.id)
                file_logger.warning("Component '%s' CANCELLED", nxt.name)

    def _mark_unrunnable(
        self,
        components: dict[str, Component],
        pending: set,
        cancelled: set,
        file_logger: logging.Logger,
    ) -> None:
        for pid in list(pending):
            comp = components[pid]
            comp.status = RuntimeState.CANCELLED
            cancelled.add(pid)
            file_logger.warning(
                "Component '%s' CANCELLED (no runnable path)", comp.name
            )

    def _finalize_success(
        self,
        job: Job,
        execution: JobExecution,
        succeeded: set,
        failed: set,
        cancelled: set,
    ) -> None:
        if failed:
            raise RuntimeError(
                "One or more components failed; dependent components cancelled"
            )
        duration = datetime.datetime.now() - execution.job_metrics.started_at
        execution.job_metrics.processing_time = duration
        execution.job_metrics.error_count = 0
        execution.job_metrics.status = RuntimeState.SUCCESS.value
        self.job_information_handler.metrics_handler.add_job_metrics(
            job.id, execution.job_metrics
        )
        for cid, comp in job.components.items():
            self.job_information_handler.metrics_handler.add_component_metrics(
                job.id, cid, comp.metrics
            )
        execution.status = RuntimeState.SUCCESS.value

    def _finalize_failure(
        self,
        job: Job,
        execution: JobExecution,
        exc: Exception,
        file_logger: logging.Logger,
    ) -> None:
        execution.status = RuntimeState.FAILED.value
        execution.job_metrics.status = RuntimeState.FAILED.value
        execution.error = str(exc)

    def _execute_component(self, component: Component, data: Any) -> Any:
        """
        Execute a single component
        !!note: will only work when component is a concrete class !!
        :param component: The component to execute
        :param data: Input data for the component
        :return: Result of the component execution
        """
        try:
            logger.info(f"Executing component: {component.name}")
            component.status = RuntimeState.RUNNING

            # Execute the component
            result = component.execute(data)

            component.status = RuntimeState.SUCCESS
            logger.info(f"Component {component.name} completed successfully")
            return result

        except Exception as e:
            logger.exception(f"Component {component.name} failed: {str(e)}")
            component.status = RuntimeState.FAILED
            raise
