import asyncio
import logging
import threading
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

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
from etl_core.components.databases.pool_registry import ConnectionPoolRegistry


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
        from etl_core.singletons import (
            execution_records_handler,
        )  # avoid circular import

        self.logger = logging.getLogger("job.ExecutionHandler")
        self._file_logger = logging.getLogger("job.FileLogger")
        self.job_info = JobInformationHandler(job_name="no_job_assigned")
        self.system_metrics_handler = SystemMetricsHandler()
        self._exec_records_handler = execution_records_handler()

    def execute_job(
        self,
        job: RuntimeJob,
        environment: Optional[Environment | str] = None,
    ) -> JobExecution:
        """
        Top-level synchronous entrypoint. Runs the async pipeline inside a
        temporary event loop (CLI, tests, sync API handlers).
        """
        execution = self._begin_execution(job, environment)
        try:
            result = asyncio.run(self._main_loop(execution))
            return result
        finally:
            self._cleanup_after_execution(execution)
            self._release_execution(job.id)

    async def execute_job_async(
        self,
        job: RuntimeJob,
        environment: Optional[Environment | str] = None,
    ) -> JobExecution:
        """
        Async variant that reuses the caller's event loop. Required for
        scheduler-driven runs that share the FastAPI loop.
        """
        execution = self._begin_execution(job, environment)
        try:
            result = await self._main_loop(execution)
            return result
        finally:
            self._cleanup_after_execution(execution)
            self._release_execution(job.id)

    def _begin_execution(
        self,
        job: RuntimeJob,
        environment: Optional[Environment | str],
    ) -> JobExecution:
        env_obj = self._normalize_environment(environment)

        with self._guard_lock:
            if job.id in self._running_jobs:
                self.logger.warning("Job '%s' is already running", job.name)
                raise ExecutionAlreadyRunning(
                    f"Job '{job.name}' ({job.id}) is already running."
                )
            self._running_jobs.add(job.id)
            execution = JobExecution(job, environment=env_obj)

        try:
            self._exec_records_handler.create_execution(
                execution_id=execution.id,
                job_id=job.id,
                environment=env_obj.value if env_obj else None,
            )
        except Exception:
            self.logger.exception("Failed to persist execution start")

        return execution

    def _release_execution(self, job_id: str) -> None:
        with self._guard_lock:
            self._running_jobs.discard(job_id)

    def _cleanup_after_execution(self, execution: JobExecution) -> None:
        job = execution.job

        for comp in job.components:
            cleanup = getattr(comp, "cleanup_after_execution", None)
            if callable(cleanup):
                try:
                    cleanup()
                except Exception:
                    self.logger.exception(
                        "Component cleanup failed for %s", comp.name
                    )

        try:
            closed = ConnectionPoolRegistry.instance().close_idle_pools()
            if closed["sql"] or closed["mongo"]:
                self.logger.debug(
                    "Closed idle pools after execution: sql=%s mongo=%s",
                    closed["sql"],
                    closed["mongo"],
                )
        except Exception:
            self.logger.exception("Failed to close idle connection pools")

    @staticmethod
    def _normalize_environment(
        environment: Optional[Environment | str],
    ) -> Optional[Environment]:
        if isinstance(environment, Environment):
            return environment
        if isinstance(environment, str):
            try:
                return Environment(environment)
            except ValueError:
                return None
        return environment

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

    def _persist_attempt_start(self, execution: JobExecution) -> None:
        attempt = execution.latest_attempt()
        try:
            self._exec_records_handler.start_attempt(
                attempt_id=attempt.id,
                execution_id=execution.id,
                attempt_index=attempt.index,
            )
        except Exception:  # pragma: no cover
            self.logger.exception("Failed to persist attempt start")

    def _persist_attempt_finish(
        self, execution: JobExecution, status: str, error: Optional[str]
    ) -> None:
        attempt = execution.latest_attempt()
        try:
            self._exec_records_handler.finish_attempt(
                attempt_id=attempt.id,
                status=status,
                error=error,
            )
        except Exception:  # pragma: no cover
            suffix = "SUCCESS" if status == "SUCCESS" else "FAILED"
            self.logger.exception("Failed to persist attempt finish (%s)", suffix)

    def _extract_exception(self, err: BaseException) -> BaseException:
        if isinstance(err, ExceptionGroup):
            return err.exceptions[0] if err.exceptions else err
        return err

    def _should_retry(self, execution: JobExecution, attempt_index: int) -> bool:
        return execution.retry_strategy.should_retry(attempt_index)

    async def _maybe_wait_before_retry(
        self, execution: JobExecution, attempt_index: int
    ) -> None:
        delay = execution.retry_strategy.next_delay(attempt_index)
        if delay > 0:
            await asyncio.sleep(delay)

    async def _run_attempt(self, execution: JobExecution) -> None:
        await self._run_latest_attempt(execution)

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
            attempt = execution.latest_attempt()

            # record attempt start
            self._persist_attempt_start(execution)
            self._file_logger.debug(
                "Attempt %d for job '%s'", attempt_index + 1, job.name
            )

            try:
                await self._run_attempt(execution)
            except BaseException as err:
                inner = self._extract_exception(err)
                attempt.error = str(inner)
                self.logger.warning(
                    "Attempt %d failed for job '%s': %s",
                    attempt.index,
                    job.name,
                    inner,
                )
                self._file_logger.warning("Attempt %d failed: %s", attempt.index, inner)
                self._persist_attempt_finish(
                    execution, status="FAILED", error=str(inner)
                )

                if not self._should_retry(execution, attempt_index):
                    self._finalize_failure(inner, execution, job_metrics)
                    break

                await self._maybe_wait_before_retry(execution, attempt_index)
                continue

            # success path
            self._persist_attempt_finish(execution, status="SUCCESS", error=None)
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
        # pred_to_in_ports_by_component: dst_comp_name -> {pred_comp_id: Deque[in_port]}
        pred_to_in_ports_by_component: Dict[str, Dict[str, Deque[str]]] = {
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
                    # remember every destination in_port this predecessor feeds
                    pred_map = pred_to_in_ports_by_component.setdefault(dst.name, {})
                    in_ports_queue = pred_map.setdefault(comp.id, deque())
                    in_ports_queue.append(in_port)
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
                pred_map = {
                    pred_id: deque(ports)
                    for pred_id, ports in pred_to_in_ports_by_component.get(
                        comp.name, {}
                    ).items()
                }
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
        pred_to_in_ports: Dict[str, Deque[str]],
    ) -> None:
        """
        Async worker loop per component.

        Args:
            pred_to_in_ports: Maps predecessor component IDs to a deque of
                destination in-port names they feed, preserving routing order
                for components that expect multiple inputs.
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
                    component, metrics, in_queues, out_edges_by_port, pred_to_in_ports
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
            metrics.update_processing_time()
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
        status = metrics.status
        try:
            current_state = (
                status
                if isinstance(status, RuntimeState)
                else RuntimeState(str(status))
            )
        except ValueError:
            current_state = RuntimeState.PENDING

        if current_state == RuntimeState.PENDING:
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

    def _resolve_single_in_port(self, component: Component) -> Optional[str]:
        names = component.expected_in_port_names()
        if len(names) == 1:
            return names[0]
        return None

    def _requires_tagged_input(self, component: Component) -> bool:
        meth = getattr(component, "requires_tagged_input", None)
        if callable(meth):
            try:
                return bool(meth())
            except Exception:
                return False
        return False

    async def _handle_sentinel_item(
        self,
        item: Sentinel,
        component: Component,
        metrics: ComponentMetrics,
        out_edges_by_port: Dict[str, List[Tuple[asyncio.Queue, str, bool]]],
        pred_to_in_ports: Dict[str, Deque[str]],
        requires_tagged: bool,
        remaining_counts: Dict[str, int],
    ) -> None:
        pred_id = item.component_id
        if pred_id not in remaining_counts:
            return

        in_ports_queue = pred_to_in_ports.get(pred_id)
        in_port: Optional[str] = None
        if in_ports_queue:
            try:
                in_port = in_ports_queue.popleft()
            except IndexError:
                in_port = None

        outstanding = remaining_counts[pred_id] - 1
        if outstanding <= 0:
            remaining_counts.pop(pred_id, None)
        else:
            remaining_counts[pred_id] = outstanding

        if not requires_tagged:
            return

        if in_port:
            await self._run_component(
                component, InTagged(in_port, Ellipsis), metrics, out_edges_by_port
            )

    async def _handle_tagged_item(
        self,
        item: InTagged,
        component: Component,
        metrics: ComponentMetrics,
        out_edges_by_port: Dict[str, List[Tuple[asyncio.Queue, str, bool]]],
        requires_tagged: bool,
    ) -> None:
        if requires_tagged:
            await self._run_component(component, item, metrics, out_edges_by_port)
            return
        dest_port = item.in_port
        payload = item.payload
        component.validate_in_payload(dest_port, payload)
        await self._run_component(component, payload, metrics, out_edges_by_port)

    async def _handle_untagged_item(
        self,
        item: Any,
        component: Component,
        metrics: ComponentMetrics,
        out_edges_by_port: Dict[str, List[Tuple[asyncio.Queue, str, bool]]],
        single_in_port: Optional[str],
        in_port_names: List[str],
    ) -> None:
        if single_in_port is None:
            raise ValueError(
                f"{component.name}: received untagged input but component "
                f"declares multiple input ports {in_port_names!r}; "
                "fan-in must use tagged envelopes."
            )
        component.validate_in_payload(single_in_port, item)
        await self._run_component(component, item, metrics, out_edges_by_port)

    def _initial_remaining_counts(
        self,
        component: Component,
        pred_to_in_ports: Dict[str, Deque[str]],
    ) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for pred in component.prev_components:
            ports = pred_to_in_ports.get(pred.id)
            if ports:
                counts[pred.id] = len(ports)
            else:
                pred_name = getattr(pred, "name", pred.id)
                raise RuntimeError(
                    f"Wiring invariant violated: component '{component.name}' "
                    f"missing in-port mapping from predecessor '{pred_name}'."
                )
        return counts

    async def _consume_and_run(
        self,
        component: Component,
        metrics: ComponentMetrics,
        in_queues: List[asyncio.Queue],
        out_edges_by_port: Dict[str, List[Tuple[asyncio.Queue, str, bool]]],
        pred_to_in_ports: Dict[str, Deque[str]],
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
        remaining = self._initial_remaining_counts(component, pred_to_in_ports)

        # Resolve the single expected in-port name if applicable
        in_port_names = component.expected_in_port_names()
        single_in_port = self._resolve_single_in_port(component)
        requires_tagged = self._requires_tagged_input(component)

        while remaining:
            item = await queue.get()

            if isinstance(item, Sentinel):
                await self._handle_sentinel_item(
                    item=item,
                    component=component,
                    metrics=metrics,
                    out_edges_by_port=out_edges_by_port,
                    pred_to_in_ports=pred_to_in_ports,
                    requires_tagged=requires_tagged,
                    remaining_counts=remaining,
                )
                continue

            if isinstance(item, InTagged):
                await self._handle_tagged_item(
                    item=item,
                    component=component,
                    metrics=metrics,
                    out_edges_by_port=out_edges_by_port,
                    requires_tagged=requires_tagged,
                )
                continue

            await self._handle_untagged_item(
                item=item,
                component=component,
                metrics=metrics,
                out_edges_by_port=out_edges_by_port,
                single_in_port=single_in_port,
                in_port_names=in_port_names,
            )

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

        try:
            self._exec_records_handler.finalize_execution(
                execution_id=execution.id,
                status="SUCCESS",
                error=None,
            )
        except Exception:  # pragma: no cover
            self.logger.exception("Failed to persist execution finalize (SUCCESS)")

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
        try:
            self._exec_records_handler.finalize_execution(
                execution_id=execution.id,
                status="FAILED",
                error=str(exc),
            )
        except Exception:  # pragma: no cover
            self.logger.exception("Failed to persist execution finalize (FAILED)")
        # cleanup
        self.logger.error(
            "Job '%s' failed after %d attempts: %s",
            execution.job.name,
            attempt.index,
            exc,
        )
