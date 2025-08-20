import asyncio
import logging
import threading
from typing import Any, Dict, List, Set
from collections import deque

from etl_core.components.runtime_state import RuntimeState
from etl_core.job_execution.runtimejob import RuntimeJob, JobExecution, Sentinel
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.components.base_component import Component
from etl_core.job_execution.job_information_handler import JobInformationHandler
from etl_core.metrics.system_metrics import SystemMetricsHandler
from etl_core.metrics.metrics_registry import get_metrics_class
from etl_core.metrics.execution_metrics import ExecutionMetrics


class ExecutionAlreadyRunning(Exception):
    """
    Raised when attempting to start an execution for a job that is already running.
    """


class JobExecutionHandler:
    """
    Manages executions of multiple Jobs in streaming mode:
    ...
    """

    # process-wide storage for job-ids of currently running jobs
    _running_jobs: Set[str] = set()
    # guard locks to ensure thread-safety
    _guard_lock = threading.Lock()

    def __init__(self) -> None:
        self.logger = logging.getLogger("job.ExecutionHandler")
        self._file_logger = logging.getLogger("job.FileLogger")
        self.job_info = JobInformationHandler(job_name="no_job_assigned")
        self.system_metrics_handler = SystemMetricsHandler()

    def execute_job(self, job: RuntimeJob) -> JobExecution:
        """
        top-level method to execute a Job, managing its execution lifecycle
        """
        with self._guard_lock:
            if job.id in self._running_jobs:
                self.logger.warning("Job '%s' is already running", job.name)
                raise ExecutionAlreadyRunning(
                    f"Job '{job.name}' ({job.id}) is already running."
                )
            # mark as running and create the execution instance
            self._running_jobs.add(job.id)
            execution = JobExecution(job)

        try:
            return asyncio.run(self._main_loop(execution))
        finally:
            # cleanup in both success/failure paths
            with self._guard_lock:
                self._running_jobs.discard(job.id)

    async def _main_loop(self, execution: JobExecution) -> JobExecution:
        """
        Main loop for executing a JobExecution
        - initializes job metrics
        - runs attempts until success or max retries exhausted
        - handles retries and finalization further
        :param execution: Execution instance to run
        """
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
                # build and run worker tasks, storing them for cancellation
                await self._run_latest_attempt(execution)
            except ExceptionGroup as eg:
                # unwrap the error from TaskGroup
                inner = eg.exceptions[0] if eg.exceptions else eg
                attempt = execution.latest_attempt()
                attempt.error = str(inner)
                self.logger.warning(
                    "Attempt %d failed for job '%s': %s",
                    attempt.index,
                    job.name,
                    inner,
                )
                self._file_logger.warning("Attempt %d failed: %s", attempt.index, inner)
                # reuse inner for retry/finalize logic
                exc = inner

                # if no retries left, finalize as failure
                if not execution.retry_strategy.should_retry(attempt_index):
                    self._finalize_failure(exc, execution, job_metrics)
                    break
                # otherwise wait and retry
                delay = execution.retry_strategy.next_delay(attempt_index)
                if delay > 0:
                    await asyncio.sleep(delay)
                continue
            else:
                # success: mark and finalize
                job_metrics.status = RuntimeState.SUCCESS
                self._finalize_success(execution, job_metrics)
                break

        return execution

    async def _run_latest_attempt(
        self, execution: JobExecution
    ) -> Dict[str, asyncio.Task]:
        """
        Launch one Task per component, track them in a dict, and await them.
        Returns the mapping from component.id to Task so failures can cancel
        downstream tasksin case of component failure.
        """
        job = execution.job

        # Prepare queues
        queues: Dict[str, asyncio.Queue] = {
            comp.name: asyncio.Queue() for comp in job.components
        }

        # spawn all component workers under one TaskGroup
        async with asyncio.TaskGroup() as tg:
            for comp in job.components:
                out_queues = [queues[n.name] for n in comp.next_components]
                in_queues = [queues[comp.name]] if comp.prev_components else []
                metrics_cls = get_metrics_class(comp.comp_type)
                metrics = self.job_info.metrics_handler.create_component_metrics(
                    execution.id,
                    execution.latest_attempt().id,
                    comp.id,
                    metrics_cls,
                )
                task = tg.create_task(
                    self._worker(execution, comp, in_queues, out_queues, metrics),
                    name=f"worker-{comp.name}",
                )
                execution.latest_attempt().current_tasks[comp.id] = task

        # once the with block exits, all workers are done (or cancelled)
        return execution.latest_attempt().current_tasks

    async def _worker(
        self,
        execution: JobExecution,
        component: Component,
        in_queues: List[asyncio.Queue],
        out_queues: List[asyncio.Queue],
        metrics: ComponentMetrics,
    ) -> None:
        """
        Runs one component; if upstream cancellation has already marked this
        metrics as CANCELLED, we just fan out the sentinel.
        """
        attempt = execution.latest_attempt()
        sentinel = execution.sentinels[component.id]

        # if already marked canceled, short-circuit
        if metrics.status == RuntimeState.CANCELLED:
            await self._fan_out(sentinel, out_queues)
            return

        try:
            # run without payload, comp is a root
            if not in_queues:
                await self._run_component(component, None, metrics, out_queues)
            else:
                await self._merge_and_run(component, metrics, in_queues, out_queues)

        except asyncio.CancelledError:
            # mark cancelled in our metrics, then re-raise so upstream sees it
            metrics.status = RuntimeState.CANCELLED
            raise

        except Exception as exc:
            # component failure: mark FAILED, increment error, cancel successors
            self._handle_worker_exception(component, exc, metrics, execution, attempt)
            raise

        else:
            if metrics.status != RuntimeState.CANCELLED:
                metrics.status = RuntimeState.SUCCESS

        finally:
            # always tell downstream there's no more data
            await self._fan_out(sentinel, out_queues)

    def _handle_worker_exception(
        self,
        component: Component,
        exc: Exception,
        metrics: ComponentMetrics,
        execution: JobExecution,
        attempt: Any,
    ) -> None:
        """
        Handle exceptions raised by a component worker
        - mark component as FAILED in metrics
        - increment error count
        - cancel all downstream components
        :return:
        """
        metrics.status = RuntimeState.FAILED
        metrics.error_count += 1
        self._file_logger.error(
            "Component '%s' FAILED: %s", component.name, exc, exc_info=True
        )
        # cancel and mark all downstream components
        self._cancel_successors(component, execution, attempt)

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
            if dm.status not in (RuntimeState.SUCCESS, RuntimeState.FAILED):
                dm.status = RuntimeState.CANCELLED
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
        """
        Run a single component with the given payload and metrics
        """
        # mark start and stream all batches
        metrics.set_started()
        async for batch in component.execute(payload, metrics):
            await self._fan_out(batch, out_queues)

    async def _merge_and_run(self, component, metrics, in_queues, out_queues):
        """
        Merge payloads from multiple input queues and run the component.
        """
        queue = in_queues[0]
        remaining = {p.id for p in component.prev_components}

        while remaining:
            item = await queue.get()
            if isinstance(item, Sentinel):
                remaining.discard(item.component_id)
            else:
                await self._run_component(component, item, metrics, out_queues)

    def _cancel_successors(
        self,
        component: Component,
        execution: JobExecution,
        attempt: Any,
    ) -> None:
        """
        BFS through downstream components: mark each CANCELLED in metrics
        and use task.cancel() to stop its async worker.
        """
        dq = deque(component.next_components)
        seen: Set[str] = set()

        while dq:
            nxt = dq.popleft()
            if nxt.id in seen:
                continue
            seen.add(nxt.id)

            # mark cancelled in metrics
            dm = self.job_info.metrics_handler.get_comp_metrics(
                execution.id, attempt.id, nxt.id
            )
            if dm.status not in (RuntimeState.SUCCESS, RuntimeState.FAILED):
                dm.status = RuntimeState.CANCELLED

            # cancel tasks cleanly
            task = execution.latest_attempt().current_tasks.get(nxt.id)
            if task and not task.done():
                task.cancel()

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
                execution.id, execution.latest_attempt().id, comp.id
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
                execution.id, execution.latest_attempt().id, comp.id
            )
            self.job_info.logging_handler.log(cm)

        # cleanup
        self.logger.info("Job '%s' completed successfully", execution.job.name)

    def _finalize_failure(
        self, exc: Exception, execution: JobExecution, job_metrics: "ExecutionMetrics"
    ) -> None:
        """
        Final actions when all retries are exhausted or streaming execution fails.
        """
        attempt = execution.latest_attempt()
        job_metrics.status = RuntimeState.FAILED
        attempt.error = str(exc)
        # cleanup
        self.logger.error(
            "Job '%s' failed after %d attempts: %s",
            execution.job.name,
            attempt.index,
            exc,
        )
