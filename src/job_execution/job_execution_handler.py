import asyncio
import logging
from typing import Any, Dict, List
from collections import deque

from src.job_execution.job import Job, JobExecution
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.components.base_component import Component
from src.job_execution.job_information_handler import JobInformationHandler
from src.metrics.system_metrics import SystemMetricsHandler
from src.metrics.metrics_registry import get_metrics_class
from src.metrics.execution_metrics import ExecutionMetrics


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

    def execute_job(self, job: Job) -> JobExecution:
        """
        Kick off a streaming execution for the given Job.
        Retries according to job.num_of_retries.
        """
        # guard against parallel execution of same job
        for exec_ in self.running_executions:
            if exec_.job == job:
                self.logger.warning(
                    "Job '%s' is already running; returning existing execution",
                    job.name,
                )
                return exec_

        # prepare new execution
        execution = JobExecution(job=job)
        self.running_executions.append(execution)

        # update logging context
        self.job_information_handler.logging_handler.update_job_name(job.name)
        self.logger.info("Starting streaming execution of job '%s'", job.name)

        job_metrics = self.job_information_handler.metrics_handler.create_job_metrics(
            execution.id
        )
        # attempt loop
        while len(execution.attempts) < execution.max_attempts:
            attempt_index = len(execution.attempts)
            execution.start_attempt()
            self._file_logger.debug(
                "Attempt %d for job '%s'", attempt_index + 1, job.name
            )
            try:
                # run one streaming attempt
                asyncio.run(self._run_latest_attempt(execution))
                # finalize on success
                job_metrics.status = "SUCCESS"
                self._finalize_success(execution, job_metrics)
                return execution

            except Exception as exc:
                attempt = execution.attempts[-1]
                # record failure and optionally retry
                attempt.error = str(exc)
                self.logger.warning(
                    "Attempt %d failed for job '%s': %s",
                    attempt.index,
                    job.name,
                    exc,
                )
                self._file_logger.warning(
                    "Attempt %d failed: %s", attempt_index + 1, exc
                )
                if attempt_index == execution.max_attempts - 1:
                    self._finalize_failure(exc, execution, job_metrics)
                    return execution

        return execution

    async def _run_latest_attempt(
        self,
        execution: JobExecution,
    ) -> None:
        """
        Executes a single attempt of a JobExecution in streaming mode.
        """
        job = execution.job
        # initialize one queue per component to hold its incoming batches
        queues: Dict[str, asyncio.Queue] = {
            comp.name: asyncio.Queue() for comp in job.components
        }

        # spawn one worker per component
        tasks: List[asyncio.Task] = []
        for comp in job.components:
            # fan outputs into the input queues of downstream components
            out_queues: List[asyncio.Queue] = [
                queues[nxt.name] for nxt in comp.next_components
            ]
            # consume from this component’s own queue if it has predecessors
            in_queues: List[asyncio.Queue] = (
                [queues[comp.name]] if comp.prev_components else []
            )

            metrics_cls = get_metrics_class(comp.comp_type)
            metrics = (
                self.job_information_handler.metrics_handler.create_component_metrics(
                    execution.id, execution.attempts[-1].id, comp.id, metrics_cls
                )
            )
            task = asyncio.create_task(
                self._worker(execution, comp, in_queues, out_queues, metrics),
                name=f"worker-{comp.name}",
            )
            tasks.append(task)

        # wait for all component workers to complete
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
        component: Component,
        in_queues: List[asyncio.Queue],
        out_queues: List[asyncio.Queue],
        metrics: ComponentMetrics,
    ) -> None:
        """
        If in_queues is empty, run once with payload=None (root).
        Otherwise, merge inputs until all send _SENTRY. On any batch,
        invoke component.execute() and fan out. On error, cancel successors.
        Always send one _SENTRY downstream.
        """
        attempt = execution.attempts[-1]
        # if a predecessor failure already cancelled this component, skip it entirely
        if metrics.status == "CANCELLED":
            await self._fan_out(_SENTRY, out_queues)
            return

        async def _run(payload: Any) -> None:
            metrics.set_started()
            async for batch in component.execute(payload, metrics):
                await self._fan_out(batch, out_queues)
            # collect component metrics to update job metrics
            all_comp_metrics = {
                comp.id: self.job_information_handler.metrics_handler.get_comp_metrics(
                    execution.id, attempt.id, comp.id
                )
                for comp in execution.job.components
            }

            # update job metrics with collected component metrics
            (
                self.job_information_handler.metrics_handler.get_job_metrics(
                    execution.id
                ).update_metrics(all_comp_metrics)
            )

        def _cancel() -> None:
            dq = deque(component.next_components)
            seen = set()
            while dq:
                nxt = dq.popleft()
                if nxt.id in seen:
                    continue
                seen.add(nxt.id)
                dm = self.job_information_handler.metrics_handler.get_comp_metrics(
                    execution.id, attempt.id, nxt.id
                )
                if dm.status not in ("SUCCESS", "FAILED"):
                    dm.status = "CANCELLED"
                dq.extend(nxt.next_components)

        try:
            if not in_queues:
                # root component
                await _run(None)
            else:
                # fan‐in: merge until all upstreams send the sentinel
                active = set(in_queues)
                while active:
                    tasks = {asyncio.create_task(q.get()): q for q in active}
                    done, _ = await asyncio.wait(
                        tasks, return_when=asyncio.FIRST_COMPLETED
                    )
                    task = done.pop()
                    src = tasks[task]
                    val = task.result()
                    if val is _SENTRY:
                        active.remove(src)
                    else:
                        await _run(val)
        except Exception as e:
            metrics.status = "FAILED"
            metrics.error_count += 1
            self._file_logger.error(
                "Component '%s' FAILED: %s", component.name, e, exc_info=True
            )
            _cancel()
            raise
        else:
            # only mark SUCCESS if we haven't been CANCELLED by an upstream
            if metrics.status != "CANCELLED":
                metrics.status = "SUCCESS"
        finally:
            # always propagate the sentinel exactly once
            await self._fan_out(_SENTRY, out_queues)

    def _finalize_success(
        self, execution: JobExecution, job_metrics: "ExecutionMetrics"
    ) -> None:
        """
        Final actions when a streaming execution succeeds.
        """
        # log job-level metrics
        self.job_information_handler.logging_handler.log(job_metrics)
        # log component metrics
        for comp in execution.job.components:
            cm = self.job_information_handler.metrics_handler.get_comp_metrics(
                execution.id, execution.attempts[-1].id, comp.id
            )
            self.job_information_handler.logging_handler.log(cm)

        # cleanup
        self.running_executions.remove(execution)
        self.logger.info("Job '%s' completed successfully", execution.job.name)

    def _finalize_failure(
        self, exc: Exception, execution: JobExecution, job_metrics: "ExecutionMetrics"
    ) -> None:
        """
        Final actions when all retries are exhausted or streaming execution fails.
        """
        attempt = execution.attempts[-1]
        job_metrics.status = "FAILED"
        attempt.error = str(exc)
        # cleanup
        self.running_executions.remove(execution)
        self.logger.error(
            "Job '%s' failed after %d attempts: %s",
            execution.job.name,
            attempt.index,
            exc,
        )
