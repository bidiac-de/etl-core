import asyncio
import logging
from typing import Any, Dict, List, Set, Tuple
from collections import deque

from src.etl_core.components.runtime_state import RuntimeState
from src.etl_core.job_execution.job import Job, JobExecution, _Sentinel
from src.etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from src.etl_core.components.base_component import Component
from src.etl_core.job_execution.job_information_handler import JobInformationHandler
from src.etl_core.metrics.system_metrics import SystemMetricsHandler
from src.etl_core.metrics.metrics_registry import get_metrics_class
from src.etl_core.metrics.execution_metrics import ExecutionMetrics
from src.etl_core.components.envelopes import InTagged, Out


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
    ) -> None:
        self.logger = logging.getLogger("job.ExecutionHandler")
        self._file_logger = logging.getLogger("job.FileLogger")
        self.job_info = JobInformationHandler(job_name="no_job_assigned")
        self.system_metrics_handler = SystemMetricsHandler()
        self.running_executions: List[JobExecution] = []

    def execute_job(self, job: Job) -> JobExecution:
        """
        top-level method to execute a Job, managing its execution lifecycle:
        - checks if the job is already running
        - initializes a JobExecution instance
        - starts the main loop for the job execution
        - handles retries and finalization
        :param job: Job instance to execute
        :return: Execution instance containing job execution details
        """
        # guard condition: dont allow executing the same job multiple times concurrently
        for exec_ in self.running_executions:
            if exec_.job == job:
                self.logger.warning("Job '%s' already running", job.name)
                return exec_

        execution = JobExecution(job)
        self.running_executions.append(execution)

        return asyncio.run(self._main_loop(execution))

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

    async def _run_latest_attempt(self, execution) -> Dict[str, asyncio.Task]:
        """
        Wire queues, create workers, and store tasks for cancellation.
        """
        job = execution.job

        # Single inbound queue per component
        in_queues: Dict[str, asyncio.Queue] = {
            comp.name: asyncio.Queue() for comp in job.components
        }

        # out_edges: comp_name -> out_port ->
        # List[(dest_queue, dest_in_port, needs_tag)]
        out_edges: Dict[str, Dict[str, List[Tuple[asyncio.Queue, str, bool]]]] = {}

        for comp in job.components:
            by_port: Dict[str, List[Tuple[asyncio.Queue, str, bool]]] = {}
            for outp, targets in comp.out_routes.items():
                triples: List[Tuple[asyncio.Queue, str, bool]] = []
                in_ports = comp.out_edges_in_ports.get(outp, [])
                for dst, in_port in zip(targets, in_ports):
                    # destination declares multi-input?
                    multi_in = len(dst.expected_in_port_names()) > 1
                    triples.append((in_queues[dst.name], in_port, multi_in))
                by_port[outp] = triples
            out_edges[comp.name] = by_port

        async with asyncio.TaskGroup() as tg:
            for comp in job.components:
                inputs = [in_queues[comp.name]] if comp.prev_components else []
                outputs = out_edges[comp.name]
                metrics = self.job_info.metrics_handler.create_component_metrics(
                    execution.id,
                    execution.latest_attempt().id,
                    comp.id,
                    get_metrics_class(comp.comp_type),
                )
                task = tg.create_task(
                    self._worker(execution, comp, inputs, outputs, metrics),
                    name=f"worker-{comp.name}",
                )
                execution.latest_attempt().current_tasks[comp.id] = task

        return execution.latest_attempt().current_tasks

    async def _worker(
            self,
            execution,
            component: Component,
            in_queues: List[asyncio.Queue],
            out_edges_by_port: Dict[str, List[Tuple[asyncio.Queue, str, bool]]],
            metrics: ComponentMetrics,
    ) -> None:
        """
        async Worker loop per component.
        """
        attempt = execution.latest_attempt()
        sentinel = execution.sentinels[component.id]

        if metrics.status == RuntimeState.CANCELLED:
            await self._broadcast_to_next_inputs(sentinel, out_edges_by_port)
            return

        try:
            if not in_queues:
                await self._run_component(component, None, metrics, out_edges_by_port)
            else:
                await self._consume_and_run(
                    component, metrics, in_queues, out_edges_by_port
                )
        except asyncio.CancelledError:
            metrics.status = RuntimeState.CANCELLED
            raise
        except Exception as exc:
            self._handle_worker_exception(component, exc, metrics, execution, attempt)
            raise
        else:
            if metrics.status != RuntimeState.CANCELLED:
                metrics.status = RuntimeState.SUCCESS
        finally:
            await self._broadcast_to_next_inputs(sentinel, out_edges_by_port)

    async def _broadcast_to_next_inputs(
            self, item: Any, edges: Dict[str, List[Tuple[asyncio.Queue, str, bool]]]
    ) -> None:
        """
        Fan-out an item to all successor input queues.
        """
        for pairs in edges.values():
            for q, _in_port, needs_tag in pairs:
                await q.put(item)

    async def _run_component(
            self,
            component: Component,
            payload: Any,
            metrics: ComponentMetrics,
            out_edges_by_port: Dict[str, List[Tuple[asyncio.Queue, str, bool]]],
    ) -> None:
        """
        Execute the component and route its results.
        """
        metrics.set_started()
        async for batch in component.execute(payload, metrics):
            if not isinstance(batch, Out):
                raise TypeError(
                    f"{component.name} must yield Out(port, payload) with port routing"
                )
            for q, dest_in, needs_tag in out_edges_by_port.get(batch.port, []):
                await q.put(
                    batch.payload if not needs_tag else InTagged(dest_in, batch.payload)
                )

    async def _consume_and_run(
            self,
            component: Component,
            metrics: ComponentMetrics,
            in_queues: List[asyncio.Queue],
            out_edges_by_port: Dict[str, List[Tuple[asyncio.Queue, str, bool]]],
    ) -> None:
        """
        Consume from a single inbound queue, handle fan-in via sentinels.
        """
        queue = in_queues[0]
        remaining = {p.id for p in component.prev_components}

        while remaining:
            item = await queue.get()

            # End-of-stream sentinel from a predecessor
            if isinstance(item, _Sentinel) and item.component_id in remaining:
                remaining.discard(item.component_id)
                continue

            # If this is a tagged envelope, unwrap to the payload
            if isinstance(item, InTagged):
                item = item.payload

            await self._run_component(component, item, metrics, out_edges_by_port)

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
        self.running_executions.remove(execution)
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
        self.running_executions.remove(execution)
        self.logger.error(
            "Job '%s' failed after %d attempts: %s",
            execution.job.name,
            attempt.index,
            exc,
        )