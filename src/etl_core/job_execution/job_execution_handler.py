import asyncio
import inspect
import logging
import threading
import traceback
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
from etl_core.context.environment import normalize_environment
from etl_core.context.template_resolver import apply_context_templates_to_component
from etl_core.errors import ExecutionConflictError
from etl_core.job_execution.execution_telemetry import execution_telemetry_store
from etl_core.components.databases.pool_registry import ConnectionPoolRegistry


class ExecutionAlreadyRunning(ExecutionConflictError):
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

    # process-wide storage for (job_id, environment) of currently running jobs
    _running_jobs: Set[Tuple[str, Optional[str]]] = set()
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
        self._telemetry = execution_telemetry_store()

    def execute_job(
        self,
        job: RuntimeJob,
        environment: Optional[str] = None,
    ) -> JobExecution:
        """
        Top-level synchronous entrypoint. Runs the async pipeline inside a
        temporary event loop (CLI, tests, sync API handlers).
        """
        execution = self._begin_execution(job, environment)
        try:
            result = asyncio.run(self._main_loop(execution))
            return result
        except BaseException as exc:
            self._telemetry.set_status(
                execution.id,
                status="FAILED",
                error=str(exc),
                finished=True,
            )
            try:
                self._exec_records_handler.finalize_execution(
                    execution_id=execution.id,
                    status="FAILED",
                    error=str(exc),
                )
            except Exception:  # pragma: no cover
                self.logger.exception(
                    "Failed to persist execution finalize (FAILED/unexpected)"
                )
            raise
        finally:
            self._cleanup_after_execution(execution)
            self._release_execution(job.id, execution.environment)

    async def execute_job_async(
        self,
        job: RuntimeJob,
        environment: Optional[str] = None,
    ) -> JobExecution:
        """
        Async variant that reuses the caller's event loop. Required for
        scheduler-driven runs that share the FastAPI loop.
        """
        execution = self._begin_execution(job, environment)
        try:
            result = await self._main_loop(execution)
            return result
        except BaseException as exc:
            self._telemetry.set_status(
                execution.id,
                status="FAILED",
                error=str(exc),
                finished=True,
            )
            try:
                self._exec_records_handler.finalize_execution(
                    execution_id=execution.id,
                    status="FAILED",
                    error=str(exc),
                )
            except Exception:  # pragma: no cover
                self.logger.exception(
                    "Failed to persist execution finalize (FAILED/unexpected async)"
                )
            raise
        finally:
            self._cleanup_after_execution(execution)
            self._release_execution(job.id, execution.environment)

    def start_job_background(
        self,
        job: RuntimeJob,
        environment: Optional[str] = None,
    ) -> JobExecution:
        """
        Start job execution in a background daemon thread and return immediately.
        """

        execution = self._begin_execution(job, environment)

        def _runner() -> None:
            try:
                self.logger.info(
                    "Background thread started for job '%s' (execution=%s)",
                    job.name,
                    execution.id,
                )
                asyncio.run(self._main_loop(execution))
            except BaseException as exc:  # noqa: BLE001
                tb_str = traceback.format_exc()
                self.logger.error(
                    "Background execution failed for job '%s' "
                    "(execution=%s): %s\n%s",
                    job.name,
                    execution.id,
                    exc,
                    tb_str,
                )
                self._telemetry.set_status(
                    execution.id,
                    status="FAILED",
                    error=str(exc),
                    finished=True,
                )
                try:
                    self._exec_records_handler.finalize_execution(
                        execution_id=execution.id,
                        status="FAILED",
                        error=str(exc),
                    )
                except Exception:  # pragma: no cover
                    self.logger.exception(
                        "Failed to persist execution finalize (FAILED/background)"
                    )
            finally:
                self._cleanup_after_execution(execution)
                self._release_execution(job.id, execution.environment)

        thread = threading.Thread(
            target=_runner,
            name=f"etl-exec-{execution.id}",
            daemon=True,
        )
        thread.start()
        return execution

    def _begin_execution(
        self,
        job: RuntimeJob,
        environment: Optional[str],
    ) -> JobExecution:
        env_obj = self._normalize_environment(environment)

        run_key = (job.id, env_obj)
        with self._guard_lock:
            if run_key in self._running_jobs:
                self.logger.warning(
                    "Job '%s' is already running in environment '%s'",
                    job.name,
                    env_obj,
                )
                raise ExecutionAlreadyRunning(
                    f"Job '{job.name}' ({job.id}) is already running"
                    f" in environment '{env_obj}'.",
                    code="EXECUTION_ALREADY_RUNNING",
                    context={
                        "job_id": job.id,
                        "job_name": job.name,
                        "environment": env_obj,
                    },
                )
            self._running_jobs.add(run_key)
            execution = JobExecution(job, environment=env_obj)

        try:
            self._exec_records_handler.create_execution(
                execution_id=execution.id,
                job_id=job.id,
                environment=env_obj,
            )
        except Exception:
            self.logger.exception("Failed to persist execution start")

        self._telemetry.start_execution(
            execution_id=execution.id,
            job_id=job.id,
            job_name=job.name,
            environment=env_obj,
            components=[(comp.id, comp.name) for comp in job.components],
        )

        return execution

    def _release_execution(
        self, job_id: str, environment: Optional[str] = None
    ) -> None:
        with self._guard_lock:
            self._running_jobs.discard((job_id, environment))

    def _cleanup_after_execution(self, execution: JobExecution) -> None:
        job = execution.job

        for comp in job.components:
            cleanup = getattr(comp, "cleanup_after_execution", None)
            if callable(cleanup):
                try:
                    cleanup()
                except Exception:
                    self.logger.exception("Component cleanup failed for %s", comp.name)

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
        environment: Optional[str],
    ) -> Optional[str]:
        if environment is None:
            return None
        return normalize_environment(environment)

    def _prepare_comps_for_execution(
        self, job: RuntimeJob, environment: Optional[str] = None
    ) -> None:
        if environment is not None:
            self.logger.info("environment set to '%s'", environment)

        from etl_core.singletons import (
            credentials_handler as _credentials_handler_singleton,
        )

        creds_repo = _credentials_handler_singleton()
        for comp in job.components:
            self.logger.debug(
                "Preparing component '%s' (%s) for execution",
                comp.name,
                comp.__class__.__name__,
            )
            try:
                apply_context_templates_to_component(
                    comp,
                    environment=environment,
                    creds_handler=creds_repo,
                )
            except Exception:
                self.logger.exception(
                    "Failed to apply context templates for component '%s'",
                    comp.name,
                )
                raise
            if environment is not None and hasattr(comp, "prepare_for_execution"):
                try:
                    comp.prepare_for_execution(environment)
                except Exception:
                    self.logger.exception(
                        "Failed to prepare component '%s' for execution",
                        comp.name,
                    )
                    raise

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
        log_path = self.job_info.logging_handler.current_log_path
        self._telemetry.set_log_path(
            execution.id,
            str(log_path) if log_path is not None else None,
        )
        self._telemetry.set_status(execution.id, status="RUNNING")
        self.logger.info("Starting execution of '%s'", job.name)

        self._prepare_comps_for_execution(
            job, execution.environment if execution.environment else None
        )
        self.logger.info("Prepared %d components for execution", len(job.components))

        job_metrics = self.job_info.metrics_handler.create_job_metrics(execution.id)

        self.logger.info("starting attempt(s) for job '%s'", job.name)
        for attempt_index in range(execution.max_attempts):
            execution.start_attempt()
            self.logger.info("started attempt %d", attempt_index + 1)
            attempt = execution.latest_attempt()
            self._telemetry.set_active_attempt(execution.id, attempt.index)
            self._telemetry.set_status(execution.id, status="RUNNING", error=None)

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

                self._telemetry.set_status(
                    execution.id,
                    status="RETRYING",
                    error=str(inner),
                )
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
                await self._run_component(
                    execution_id=execution.id,
                    component=component,
                    payload=None,
                    metrics=metrics,
                    out_edges_by_port=out_edges_by_port,
                )
            else:
                await self._consume_and_run(
                    execution_id=execution.id,
                    component=component,
                    metrics=metrics,
                    in_queues=in_queues,
                    out_edges_by_port=out_edges_by_port,
                    pred_to_in_ports=pred_to_in_ports,
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
            self._update_component_telemetry(
                execution_id=execution.id,
                component=component,
                metrics=metrics,
                last_event="worker-finalize",
            )
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

    @staticmethod
    def _runtime_state_to_str(state: Any) -> str:
        if isinstance(state, RuntimeState):
            return state.value
        return str(state)

    @staticmethod
    def _payload_row_count(payload: Any) -> int:
        if payload is None:
            return 0
        if isinstance(payload, (str, bytes)):
            return 1
        if isinstance(payload, dict):
            return 1
        if isinstance(payload, (list, tuple, set)):
            return len(payload)
        if hasattr(payload, "shape"):
            try:
                shape = payload.shape
                if isinstance(shape, tuple) and len(shape) > 0:
                    return int(shape[0])
            except Exception:
                pass
        if hasattr(payload, "__len__"):
            try:
                return int(len(payload))
            except Exception:
                return 1
        return 1

    def _update_component_telemetry(
        self,
        *,
        execution_id: str,
        component: Component,
        metrics: ComponentMetrics,
        last_event: Optional[str] = None,
    ) -> None:
        rows_received = getattr(metrics, "lines_received", 0)
        rows_forwarded = getattr(metrics, "lines_forwarded", 0)
        error_count = getattr(metrics, "error_count", 0)
        comp_status = self._runtime_state_to_str(
            getattr(metrics, "status", RuntimeState.PENDING)
        )
        self._telemetry.update_component(
            execution_id,
            component_id=component.id,
            status=comp_status,
            rows_received=int(rows_received or 0),
            rows_forwarded=int(rows_forwarded or 0),
            error_count=int(error_count or 0),
            last_event=last_event,
        )

    async def _run_component(
        self,
        execution_id: str,
        component: Component,
        payload: Any,
        metrics: ComponentMetrics,
        out_edges_by_port: Dict[str, List[Tuple[asyncio.Queue, str, bool]]],
    ) -> None:
        # mark start
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

        self._update_component_telemetry(
            execution_id=execution_id,
            component=component,
            metrics=metrics,
            last_event="component-start",
        )

        # >>> added diagnostics
        try:
            sig = str(inspect.signature(component.execute))  # e.g. (payload, metrics)
            self.logger.debug(
                "Executing component '%s' (%s.execute%s) payload_type=%s",
                component.name,
                component.__class__.__name__,
                sig,
                type(payload).__name__,
            )
        except Exception:
            self.logger.debug(
                "Executing component '%s' (signature unavailable)", component.name
            )

        async for batch in component.execute(payload, metrics):
            if not isinstance(batch, Out):
                self.logger.error(
                    "Invalid yield from '%s': %s (expected Out(port, payload))",
                    component.name,
                    type(batch).__name__,
                )
                raise TypeError(
                    f"{component.name} must yield Out(port, payload) with port routing"
                )
            edges = out_edges_by_port.get(batch.port, [])
            if edges:
                try:
                    component.validate_out_payload(batch.port, batch.payload)
                except TypeError as exc:
                    self.logger.error(
                        "Output validation TypeError"
                        " in '%s' on port '%s' with payload=%s: %s",
                        component.name,
                        batch.port,
                        type(batch.payload).__name__,
                        exc,
                    )
                    raise
            for q, dest_in, needs_tag in edges:
                await q.put(
                    batch.payload if not needs_tag else InTagged(dest_in, batch.payload)
                )
            self._update_component_telemetry(
                execution_id=execution_id,
                component=component,
                metrics=metrics,
                last_event=f"emit:{batch.port}",
            )

        self._update_component_telemetry(
            execution_id=execution_id,
            component=component,
            metrics=metrics,
            last_event="component-step-complete",
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
        execution_id: str,
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
                execution_id=execution_id,
                component=component,
                payload=InTagged(in_port, Ellipsis),
                metrics=metrics,
                out_edges_by_port=out_edges_by_port,
            )

    async def _handle_tagged_item(
        self,
        execution_id: str,
        item: InTagged,
        component: Component,
        metrics: ComponentMetrics,
        out_edges_by_port: Dict[str, List[Tuple[asyncio.Queue, str, bool]]],
        requires_tagged: bool,
    ) -> None:
        if requires_tagged:
            await self._run_component(
                execution_id=execution_id,
                component=component,
                payload=item,
                metrics=metrics,
                out_edges_by_port=out_edges_by_port,
            )
            return
        dest_port = item.in_port
        payload = item.payload
        try:
            component.validate_in_payload(dest_port, payload)
        except TypeError as exc:
            self.logger.error(
                "Output validation TypeError in '%s'"
                " on port '%s' with payload type %s: %s",
                component.name,
                dest_port,
                type(payload).__name__,
                exc,
            )
            raise
        await self._run_component(
            execution_id=execution_id,
            component=component,
            payload=payload,
            metrics=metrics,
            out_edges_by_port=out_edges_by_port,
        )

    async def _handle_untagged_item(
        self,
        execution_id: str,
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
        await self._run_component(
            execution_id=execution_id,
            component=component,
            payload=item,
            metrics=metrics,
            out_edges_by_port=out_edges_by_port,
        )

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
        execution_id: str,
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
                    execution_id=execution_id,
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
                    execution_id=execution_id,
                    item=item,
                    component=component,
                    metrics=metrics,
                    out_edges_by_port=out_edges_by_port,
                    requires_tagged=requires_tagged,
                )
                continue

            await self._handle_untagged_item(
                execution_id=execution_id,
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
        self._update_component_telemetry(
            execution_id=execution.id,
            component=component,
            metrics=metrics,
            last_event="worker-exception",
        )
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
            self._update_component_telemetry(
                execution_id=execution.id,
                component=nxt,
                metrics=dm,
                last_event="cancelled-by-upstream-failure",
            )
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
            self._update_component_telemetry(
                execution_id=execution.id,
                component=nxt,
                metrics=dm,
                last_event="cancelled",
            )

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

        self._telemetry.set_status(
            execution.id, status="SUCCESS", error=None, finished=True
        )

        # cleanup
        self.logger.info("Job '%s' completed successfully", execution.job.name)

    def _finalize_failure(
        self,
        exc: BaseException,
        execution: JobExecution,
        job_metrics: "ExecutionMetrics",
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
        self._telemetry.set_status(
            execution.id,
            status="FAILED",
            error=str(exc),
            finished=True,
        )
        # cleanup
        self.logger.error(
            "Job '%s' failed after %d attempts: %s",
            execution.job.name,
            attempt.index,
            exc,
        )
