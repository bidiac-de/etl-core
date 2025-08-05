import asyncio
import logging
from typing import Any, Dict, List, Set
from collections import deque

from src.job_execution.job import Job, JobExecution
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.components.base_component import Component
from src.job_execution.job_information_handler import JobInformationHandler
from src.metrics.system_metrics import SystemMetricsHandler
from src.metrics.metrics_registry import get_metrics_class
from src.metrics.execution_metrics import ExecutionMetrics
from src.job_execution.retry_strategy import RetryStrategy, ConstantRetryStrategy


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

    def __init__(
        self,
        retry_strategy: RetryStrategy | None = None,
    ) -> None:
        self.logger = logging.getLogger("job.ExecutionHandler")
        self._file_logger = logging.getLogger("job.FileLogger")
        self.job_info = JobInformationHandler(job_name="no_job_assigned")
        self.system_metrics_handler = SystemMetricsHandler()
        self.running_executions: List[JobExecution] = []
        # standard strategy:  retry exactly job.num_of_retries times with no delay
        self.retry_strategy = retry_strategy or ConstantRetryStrategy(0, delay=0)

    def execute_job(self, job: Job) -> JobExecution:
        # guard condition: dont allow executing the same job multiple times concurrently
        for exec_ in self.running_executions:
            if exec_.job == job:
                self.logger.warning("Job '%s' already running", job.name)
                return exec_

        execution = JobExecution(job)
        self.running_executions.append(execution)
        # fit retry strategy to job
        self.retry_strategy.max_retries = job.num_of_retries

        return asyncio.run(self._main_loop(execution))

    async def _main_loop(self, execution: JobExecution) -> JobExecution:
        job = execution.job
        self.job_info.logging_handler.update_job_name(job.name)
        self.logger.info("Starting execution of '%s'", job.name)

        job_metrics = self.job_info.metrics_handler.create_job_metrics(execution.id)

        for attempt_index in range(execution.max_attempts):
            execution.start_attempt()
            self._file_logger.debug(
                "Attempt %d for job '%s'", attempt_index + 1, job.name
            )
            try:
                await self._run_latest_attempt(execution)
            except Exception as exc:
                # failed attempt
                attempt = execution.attempts[-1]
                attempt.error = str(exc)
                self.logger.warning(
                    "Attempt %d failed for job '%s': %s",
                    attempt.index,
                    job.name,
                    exc,
                )
                self._file_logger.warning("Attempt %d failed: %s", attempt.index, exc)

                if not self.retry_strategy.should_retry(attempt_index):
                    self._finalize_failure(exc, execution, job_metrics)
                    break

                delay = self.retry_strategy.next_delay(attempt_index)
                if delay > 0:
                    await asyncio.sleep(delay)
                continue
            else:
                job_metrics.status = "SUCCESS"
                self._finalize_success(execution, job_metrics)
                break

        return execution

    async def _run_latest_attempt(self, execution: JobExecution) -> None:
        job = execution.job
        queues: Dict[str, asyncio.Queue] = {
            comp.name: asyncio.Queue() for comp in job.components
        }
        tasks = []
        for comp in job.components:
            out_queues = [queues[n.name] for n in comp.next_components]
            in_queues = [queues[comp.name]] if comp.prev_components else []
            metrics_cls = get_metrics_class(comp.comp_type)
            metrics = self.job_info.metrics_handler.create_component_metrics(
                execution.id, execution.attempts[-1].id, comp.id, metrics_cls
            )
            task = asyncio.create_task(
                self._worker(execution, comp, in_queues, out_queues, metrics),
                name=f"worker-{comp.name}",
            )
            tasks.append(task)
        await asyncio.gather(*tasks)

    async def _worker(
        self,
        execution: JobExecution,
        component: Component,
        in_queues: List[asyncio.Queue],
        out_queues: List[asyncio.Queue],
        metrics: ComponentMetrics,
    ) -> None:
        attempt = execution.attempts[-1]
        if metrics.status == "CANCELLED":
            await self._fan_out(_SENTRY, out_queues)
            return

        try:
            if not in_queues:
                await self._run_component(component, None, metrics, out_queues)
            else:
                await self._merge_and_run(component, metrics, in_queues, out_queues)
        except Exception as exc:
            self._handle_worker_exception(component, exc, metrics, execution, attempt)
            raise
        else:
            if metrics.status != "CANCELLED":
                metrics.status = "SUCCESS"
        finally:
            await self._fan_out(_SENTRY, out_queues)

    def _handle_worker_exception(
        self,
        component: Component,
        exc: Exception,
        metrics: ComponentMetrics,
        execution: JobExecution,
        attempt: Any,
    ) -> None:
        metrics.status = "FAILED"
        metrics.error_count += 1
        self._file_logger.error(
            "Component '%s' FAILED: %s", component.name, exc, exc_info=True
        )
        # cancel successors
        from collections import deque

        dq = deque(component.next_components)
        seen: Set[str] = set()
        while dq:
            nxt = dq.popleft()
            if nxt.id in seen:
                continue
            seen.add(nxt.id)
            dm = self.job_info.metrics_handler.get_comp_metrics(
                execution.id, attempt.id, nxt.id
            )
            if dm.status not in ("SUCCESS", "FAILED"):
                dm.status = "CANCELLED"
            dq.extend(nxt.next_components)

    async def _fan_out(self, item: Any, queues: List[asyncio.Queue]) -> None:
        """
        Fan out a batch or sentinel to all downstream queues.
        """
        for q in queues:
            await q.put(item)

    async def _run_component(
        self,
        component: Component,
        payload: Any,
        metrics: ComponentMetrics,
        out_queues: List[asyncio.Queue],
    ) -> None:
        # mark start and stream all batches
        metrics.set_started()
        async for batch in component.execute(payload, metrics):
            await self._fan_out(batch, out_queues)

    async def _merge_and_run(
        self,
        component: Component,
        metrics: ComponentMetrics,
        in_queues: List[asyncio.Queue],
        out_queues: List[asyncio.Queue],
    ) -> None:
        # wait for all predecessors to send the sentinel
        active = set(in_queues)
        while active:
            pending = {asyncio.create_task(q.get()): q for q in active}
            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            task = done.pop()
            src = pending[task]
            val = task.result()
            if val is _SENTRY:
                active.remove(src)
            else:
                await self._run_component(component, val, metrics, out_queues)

    def _cancel_successors(
        self,
        component: Component,
        execution_id: str,
        attempt_id: str,
    ) -> None:
        dq = deque(component.next_components)
        seen: Set[str] = set()
        while dq:
            nxt = dq.popleft()
            if nxt.id in seen:
                continue
            seen.add(nxt.id)
            dm = self.job_info.metrics_handler.get_comp_metrics(
                execution_id, attempt_id, nxt.id
            )
            if dm.status not in ("SUCCESS", "FAILED"):
                dm.status = "CANCELLED"
            dq.extend(nxt.next_components)

    def _finalize_success(
        self, execution: JobExecution, job_metrics: "ExecutionMetrics"
    ) -> None:
        """
        Final actions when a streaming execution succeeds.
        """
        # aggregate component metrics for final job metrics
        all_comp = {
            comp.id: self.job_info.metrics_handler.get_comp_metrics(
                execution.id, execution.attempts[-1].id, comp.id
            )
            for comp in execution.job.components
        }
        jm = self.job_info.metrics_handler.get_job_metrics(execution.id)
        jm.update_metrics(all_comp)

        # log job-level metrics
        self.job_info.logging_handler.log(job_metrics)
        # log component metrics
        for comp in execution.job.components:
            cm = self.job_info.metrics_handler.get_comp_metrics(
                execution.id, execution.attempts[-1].id, comp.id
            )
            self.job_info.logging_handler.log(cm)

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
