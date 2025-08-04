import asyncio
import logging
from typing import Any, Dict, List

from src.job_execution.job import Job, JobExecution, ExecutionAttempt
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.components.base_component import Component
from src.job_execution.job_information_handler import JobInformationHandler
from src.metrics.system_metrics import SystemMetricsHandler

_SENTRY = object()


class JobExecutionHandler:
    """
    Manages executions of multiple Jobs in streaming mode:
    - Maintains running executions and their attempts
    - Integrates file and console logging
    - Records system and component metrics
    - For each execution attempt, spawns one asyncio worker per component
    - Retries up to job.num_of_retries
    """

    def __init__(self) -> None:
        # logging setup
        self.logger = logging.getLogger("job.ExecutionHandler")
        self._file_logger = logging.getLogger("job.FileLogger")
        # metrics handlers
        self.job_information_handler = JobInformationHandler(job_name="no_job_assigned")
        self.system_metrics_handler = SystemMetricsHandler()
        # tracking running executions
        self.running_executions: List[JobExecution] = []

    def execute_job(self, job: Job) -> Job:
        """
        Kick off a streaming execution for the given Job.
        Retries according to job.num_of_retries.
        """
        # guard against parallel execution of same job
        if any(exec_.job == job for exec_ in self.running_executions):
            self.logger.warning(
                "Job '%s' is already running; skipping new execution", job.name
            )
            return job

        # prepare new execution
        execution = JobExecution(job=job, number_of_attempts=job.num_of_retries + 1)
        self.running_executions.append(execution)
        job.executions.append(execution)

        # update logging context
        self.job_information_handler.logging_handler.update_job_name(job.name)
        self.logger.info("Starting streaming execution of job '%s'", job.name)

        # attempt loop
        for attempt_index in range(execution.number_of_attempts):
            attempt: ExecutionAttempt = execution.create_attempt()
            self._file_logger.debug(
                "Attempt %d for job '%s'", attempt_index + 1, job.name
            )
            try:
                # run one streaming attempt
                asyncio.run(self._run_attempt(execution, attempt))
                # finalize on success
                execution.job_metrics.status = "SUCCESS"
                self._finalize_success(execution, attempt)
                return job

            except Exception as exc:
                # record failure and optionally retry
                attempt.error = str(exc)
                self.logger.warning(
                    "Attempt %d failed for job '%s': %s",
                    attempt_index + 1,
                    job.name,
                    exc,
                )
                self._file_logger.warning(
                    "Attempt %d failed: %s", attempt_index + 1, exc
                )
                if attempt_index == execution.number_of_attempts - 1:
                    self._finalize_failure(exc, execution, attempt)
                    return job

        return job

    async def _run_attempt(
        self,
        execution: JobExecution,
        attempt: ExecutionAttempt,
    ) -> None:
        """
        Executes a single attempt of a JobExecution in streaming mode.
        """
        job = execution.job
        # initialize per-component queues
        queues: Dict[str, asyncio.Queue] = {
            comp.name: asyncio.Queue() for comp in job.components
        }
        # spawn one worker per component
        tasks: List[asyncio.Task] = []
        for comp in job.components:
            # all components that follow this one in the execution chain
            downstream_names = [n.name for n in comp.next_components]
            out_queues = [queues[name] for name in downstream_names]
            # all components that precede this one in the execution chain
            upstream_names = [n.name for n in comp.prev_components]
            in_queues = [queues[name] for name in upstream_names]

            metrics: ComponentMetrics = attempt.component_metrics[comp.id]
            task = asyncio.create_task(
                self._worker(execution, attempt, comp, in_queues, out_queues, metrics),
                name=f"worker-{comp.name}",
            )
            tasks.append(task)

        # seed root components
        for root in job.root_components:
            q = queues[root.name]
            await q.put(None)
            await q.put(_SENTRY)

        # wait for streaming workers to complete
        await asyncio.gather(*tasks)

    async def _fan_out(self, item: Any, queues: List[asyncio.Queue]) -> None:
        """
        Fan out a batch or sentinel to all downstream queues.
        """
        for q in queues:
            await q.put(item)

    async def _worker(
        self,
        execution: JobExecution,
        attempt: ExecutionAttempt,
        component: Component,
        in_queues: List[asyncio.Queue],
        out_queues: List[asyncio.Queue],
        metrics: ComponentMetrics,
    ) -> None:
        """
        Worker that merges multiple input queues, streams through the component,
        and fans out to downstream queues. Shuts down only when all inputs send _SENTRY.
        """
        # Keep track of which input queues are still open
        active_queues = set(in_queues)

        try:
            while active_queues:
                # Create a get-task for each active input queue
                get_tasks = {asyncio.create_task(q.get()): q for q in active_queues}
                # Wait until any one queue has an item
                done, _ = await asyncio.wait(
                    get_tasks.keys(), return_when=asyncio.FIRST_COMPLETED
                )
                task = done.pop()
                payload = task.result()
                source_q = get_tasks[task]

                # If we got the sentinel, mark that input closed
                if payload is _SENTRY:
                    active_queues.remove(source_q)
                    continue

                # Otherwise, process the payload
                self._file_logger.info(
                    "Component '%s' processing payload", component.name
                )
                metrics.set_started()

                async for batch in component.execute(payload, metrics):
                    await self._fan_out(batch, out_queues)

                execution.job_metrics.update_metrics(attempt.component_metrics)
                metrics.status = "SUCCESS"
                self._file_logger.info("Component '%s' completed", component.name)

        except Exception as exc:
            metrics.status = "FAILED"
            metrics.error_count += 1
            self._file_logger.error(
                "Component '%s' FAILED: %s", component.name, exc, exc_info=True
            )
            raise

        finally:
            # Propagate end-of-stream once, after all inputs closed
            await self._fan_out(_SENTRY, out_queues)

    def _finalize_success(
        self, execution: JobExecution, attempt: ExecutionAttempt
    ) -> None:
        """
        Final actions when a streaming execution succeeds.
        """
        # persist job-level metrics
        self.job_information_handler.metrics_handler.add_job_metrics(
            execution.job.id, execution.job_metrics
        )
        # persist component-level metrics
        for comp in execution.job.components:
            cid = comp.id
            self.job_information_handler.metrics_handler.add_component_metrics(
                execution.job.id, cid, attempt.component_metrics[cid]
            )
        # cleanup
        self.running_executions.remove(execution)
        self.logger.info("Job '%s' completed successfully", execution.job.name)

    def _finalize_failure(
        self, exc: Exception, execution: JobExecution, attempt: ExecutionAttempt
    ) -> None:
        """
        Final actions when all retries are exhausted or streaming execution fails.
        """
        execution.job_metrics.status = "FAILED"
        attempt.error = str(exc)
        # cleanup
        self.running_executions.remove(execution)
        self.logger.error(
            "Job '%s' failed after %d attempts: %s",
            execution.job.name,
            attempt.attempt_number,
            exc,
        )
