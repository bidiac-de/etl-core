import asyncio
import logging
import threading
from typing import Any, Dict, List, Set, Tuple, Optional
from collections import deque

from etl_core.components.runtime_state import RuntimeState
from etl_core.job_execution.runtimejob import RuntimeJob, JobExecution, Sentinel
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.components.base_component import Component
from etl_core.job_execution.job_information_handler import JobInformationHandler
from etl_core.metrics.system_metrics import SystemMetricsHandler
from etl_core.metrics.metrics_registry import get_metrics_class
from etl_core.metrics.execution_metrics import ExecutionMetrics
from etl_core.components.envelopes import InTagged, Out
from etl_core.context.environment import Environment


class ExecutionAlreadyRunning(Exception):
    """
    Raised when attempting to start an execution for a job that is already running.
    """


class JobExecutionHandler:
    """
    Manages executions of multiple Jobs in streaming mode:
    - Maintains running executions and their attempts
    - Integrates file and console logging
    - Records system and component metrics
    - For each execution attempt, spawns one asyncio worker per component
    - Retries up to job.num_of_retries
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

    def execute_job(
        self,
        job: RuntimeJob,
        environment: Optional[Environment | str] = None,  # NEW
    ) -> JobExecution:
        """
        Top-level method to execute a Job, managing its execution lifecycle.
        """
        env_obj: Optional[Environment] = (
            Environment(environment) if isinstance(environment, str) else environment
        )
        with self._guard_lock:
            if job.id in self._running_jobs:
                self.logger.warning("Job '%s' is already running", job.name)
                raise ExecutionAlreadyRunning(
                    f"Job '{job.name}' ({job.id}) is already running."
                )
            # mark as running and create the execution instance
            self._running_jobs.add(job.id)
            execution = JobExecution(job, environment=env_obj)

        try:
            return asyncio.run(self._main_loop(execution))
        finally:
            # cleanup in both success/failure paths
            with self._guard_lock:
                self._running_jobs.discard(job.id)

    def _prepare_comps_for_execution(
        self, job: RuntimeJob, environment: Optional[Environment] = None
    ) -> None:
        if environment is not None:
            for comp in job.components:
                (
                    comp.prepare_for_execution(environment)
                    if hasattr(comp, "prepare_for_execution")
                    else None
                )

    async def _main_loop(self, execution: JobExecution) -> JobExecution:
        """
        Main loop for executing a JobExecution.
        """
        job = execution.job
        self.job_info.logging_handler.update_job_name(job.name)
        self.logger.info("Starting execution of '%s'", job.name)

        self._prepare_comps_for_execution(
            job, execution.environment if execution.environment else None
        )

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

        # Destination-side helper:
        # pred_to_in_port_by_component: dst_comp_name -> {pred_comp_id: in_port}
        pred_to_in_port_by_component: Dict[str, Dict[str, str]] = {
            comp.name: {} for comp in job.components
        }

        for comp in job.components:
            by_port: Dict[str, List[Tuple[asyncio.Queue, str, bool]]] = {}
            for outp, targets in comp.out_routes.items():
                triples: List[Tuple[asyncio.Queue, str, bool]] = []
                in_ports = comp.out_edges_in_ports.get(outp, [])
                for dst, in_port in zip(targets, in_ports):
                    # destination declares multi-input?
                    multi_in = len(dst.expected_in_port_names()) > 1
                    triples.append((in_queues[dst.name], in_port, multi_in))
                    # remember for the destination which in_port this predecessor feeds
                    pred_to_in_port_by_component[dst.name][comp.id] = in_port
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
                pred_map = pred_to_in_port_by_component.get(comp.name, {})
                task = tg.create_task(
                    self._worker(
                        execution,
                        comp,
                        inputs,
                        outputs,
                        metrics,
                        pred_map,
                    ),
                    name=f"worker-{comp.name}",
                )
                execution.latest_attempt().current_tasks[comp.id] = task

        return execution.latest_attempt().current_tasks

    async def _worker(
        self,
        execution: JobExecution,
        component: Component,
        in_queues: List[asyncio.Queue],
        out_edges_by_port: Dict[str, List[Tuple[asyncio.Queue, str, bool]]],
        metrics: ComponentMetrics,
        pred_to_in_port: Dict[str, str],
    ) -> None:
        """
        Async worker loop per component.
        """
        attempt = execution.latest_attempt()
        sentinel = execution.sentinels[component.id]

        # if already marked canceled, short-circuit
        if metrics.status == RuntimeState.CANCELLED:
            await self._broadcast_to_next_inputs(sentinel, out_edges_by_port)
            return

        try:
            if not in_queues:
                await self._run_component(component, None, metrics, out_edges_by_port)
            else:
                await self._consume_and_run(
                    component, metrics, in_queues, out_edges_by_port, pred_to_in_port
                )
        except asyncio.CancelledError:
            # mark cancelled in metrics, then re-raise
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
            await self._broadcast_to_next_inputs(sentinel, out_edges_by_port)

    async def _broadcast_to_next_inputs(
        self, item: Any, edges: Dict[str, List[Tuple[asyncio.Queue, str, bool]]]
    ) -> None:
        """
        Fan-out an item to all successor input queues.
        """
        for pairs in edges.values():
            for q, _in_port, _needs_tag in pairs:
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
            edges = out_edges_by_port.get(batch.port, [])
            # validate output payload if any edge receives it
            if edges:
                component.validate_out_payload(batch.port, batch.payload)

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
        pred_to_in_port: Dict[str, str],
    ) -> None:
        """
        Consume from a single inbound queue, handle fan-in via sentinels.

        - Single-input components: expect untagged payloads.
        - Multi-input components: expect InTagged(...) on the queue.
          * If the downstream component requires tagged input, we pass the
            InTagged through unchanged so it can buffer by in_port.
          * Otherwise we unwrap and validate like before.
        - When a Sentinel arrives:
          * If the downstream component **requires** tagged input, we pass a
            synthetic InTagged(in_port, Ellipsis) into the component to mark that
            port as closed. No new envelope types are introduced.
          * Otherwise we just account for the closing predecessor.
        """
        queue = in_queues[0]
        remaining = {p.id for p in component.prev_components}

        # Resolve the single expected in-port name if applicable
        in_port_names = component.expected_in_port_names()
        single_in_port: Optional[str] = None
        if len(in_port_names) == 1:
            single_in_port = in_port_names[0]

        requires_tagged = False
        meth = getattr(component, "requires_tagged_input", None)
        if callable(meth):
            try:
                requires_tagged = bool(meth())
            except Exception:
                requires_tagged = False

        while remaining:
            item = await queue.get()

            if isinstance(item, Sentinel) and item.component_id in remaining:
                remaining.discard(item.component_id)
                if requires_tagged:
                    in_port = pred_to_in_port.get(item.component_id)
                    if in_port:
                        await self._run_component(
                            component,
                            InTagged(in_port, Ellipsis),
                            metrics,
                            out_edges_by_port,
                        )
                continue

            if isinstance(item, InTagged):
                if requires_tagged:
                    await self._run_component(
                        component, item, metrics, out_edges_by_port
                    )
                    continue
                dest_port = item.in_port
                payload = item.payload
                component.validate_in_payload(dest_port, payload)
                await self._run_component(
                    component, payload, metrics, out_edges_by_port
                )
                continue

            # Untagged: only valid for single-input components
            if single_in_port is None:
                raise ValueError(
                    f"{component.name}: received untagged input but component "
                    f"declares multiple input ports {in_port_names!r}; "
                    "fan-in must use tagged envelopes."
                )
            component.validate_in_payload(single_in_port, item)
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
        Handle exceptions raised by a component worker.
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
        BFS through downstream components and cancel their tasks.
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
        Final actions when streaming execution fails.
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
