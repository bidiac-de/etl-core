from typing import List, Dict, Tuple, Any, Set, Self, Optional


from etl_core.components.dataclasses import MetaData
from pydantic import (
    Field,
    ConfigDict,
    model_validator,
    PrivateAttr,
)
import asyncio

from etl_core.components.base_component import Component, get_strategy
from etl_core.job_execution.retry_strategy import RetryStrategy, ConstantRetryStrategy
from etl_core.persistance.base_models.job_base import JobBase
from etl_core.components.wiring.ports import EdgeRef
from etl_core.utils.common_helpers import assert_unique
from etl_core.context.environment import Environment
from uuid import uuid4
import logging

logger = logging.getLogger("job.ExecutionHandler")


class Sentinel:
    """Unique end-of-stream marker for each component."""

    __slots__ = ("component_id",)

    def __init__(self, component_id: str) -> None:
        self.component_id = component_id

    def __repr__(self) -> str:
        return f"<Sentinel {self.component_id}>"


class RuntimeJob(JobBase):
    """
    Job Objects
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True, extra="ignore", validate_assignment=True
    )
    components: List[Component] = Field(default_factory=list)
    metadata_: MetaData = Field(default_factory=lambda: MetaData(), alias="metadata")
    _id: str = PrivateAttr(default_factory=lambda: str(uuid4()))

    @model_validator(mode="after")
    def _wire_and_validate(self) -> Self:
        """
        Build a port-to-port graph:
          routes: out_port -> [EdgeRef(to=..., in_port=...)]
        Then enforce:
          - unique names
          - required/fanout on outputs
          - required/fanin on inputs
          - schema presence for all *used* ports
        """
        assert_unique([c.name for c in self.components], context="component names")
        name_map = self._build_name_map()
        self._reset_runtime_links()

        incoming = self._resolve_all_routes(name_map)
        self._enforce_contracts(incoming)

        return self

    def _build_name_map(self) -> Dict[str, Component]:
        return {c.name: c for c in self.components}

    def _reset_runtime_links(self) -> None:
        for c in self.components:
            c.next_components = []
            c.prev_components = []
            # Some components store these privately; make sure they exist/reset
            if hasattr(c, "_out_routes"):
                c._out_routes = {}  # type: ignore[attr-defined]
            if hasattr(c, "_out_edges_in_ports"):
                c._out_edges_in_ports = {}  # type: ignore[attr-defined]

    def _resolve_all_routes(
        self, name_map: Dict[str, Component]
    ) -> Dict[Tuple[str, str], int]:
        """
        Resolve EdgeRefs to concrete targets and input ports.
        Returns counts of incoming edges per (target_name, in_port).
        """
        incoming: Dict[Tuple[str, str], int] = {}
        for src in self.components:
            targets, inports = self._resolve_src_routes(src, name_map, incoming)
            # persist per-source routing so the runtime can use it
            setattr(src, "_out_routes", targets)
            setattr(src, "_out_edges_in_ports", inports)
            # link graph next/prev without duplicates
            self._link_runtime_edges(src, targets)
        return incoming

    def _resolve_src_routes(
        self,
        src: Component,
        name_map: Dict[str, Component],
        incoming: Dict[Tuple[str, str], int],
    ) -> Tuple[Dict[str, List[Component]], Dict[str, List[str]]]:
        """
        Resolve all out routes of one source component.
        """
        resolved_targets: Dict[str, List[Component]] = {}
        resolved_in_ports: Dict[str, List[str]] = {}

        # resolve each out_port to its targets
        for out_port, targets in src.routes.items():
            dst_list: List[Component] = []
            in_ports: List[str] = []

            # backref targets to components and choose in_port
            for t in targets:
                ref = t if isinstance(t, EdgeRef) else EdgeRef(to=str(t))
                dst = self._lookup_target(src, out_port, ref, name_map)
                in_port = self._choose_in_port(src, dst, out_port, ref.in_port)
                self._record_incoming(incoming, dst.name, in_port)

                dst_list.append(dst)
                in_ports.append(in_port)

            resolved_targets[out_port] = dst_list
            resolved_in_ports[out_port] = in_ports

        return resolved_targets, resolved_in_ports

    def _lookup_target(
        self,
        src: Component,
        out_port: str,
        ref: "EdgeRef",
        name_map: Dict[str, Component],
    ) -> Component:
        """
        Find destination component or fail with a clear error.
        """
        dst = name_map.get(ref.to)
        if dst is None:
            raise ValueError(
                f"Unknown target {ref.to!r} in routes[{src.name}][{out_port!r}]"
            )
        return dst

    def _choose_in_port(
        self,
        src: Component,
        dst: Component,
        out_port: str,
        specified: str | None,
    ) -> str:
        """
        No implicit fallback:
        - if specified -> must be declared on dst
        - if exactly one declared -> pick it
        - if none declared -> error (roots can't be destinations)
        - if many declared and unspecified -> error
        """
        declared = getattr(dst, "expected_in_port_names", lambda: [])()
        if specified:
            if specified not in declared:
                raise ValueError(
                    f"Unknown in_port {specified!r} for target {dst.name}; "
                    f"declared inputs: {sorted(declared)}"
                )
            return specified

        if len(declared) == 1:
            return declared[0]

        if len(declared) == 0:
            raise ValueError(
                f"Target {dst.name} declares no input ports (ALLOW_NO_INPUTS=True). "
                f"It cannot be a routing destination."
            )

        raise ValueError(
            f"Target {dst.name} has multiple input ports {sorted(declared)}; "
            f"specify in_port for edge {src.name}:{out_port} -> {dst.name}"
        )

    @staticmethod
    def _record_incoming(
        incoming: Dict[Tuple[str, str], int], dst_name: str, in_port: str
    ) -> None:
        """
        Count fan-in per Tuple.
        """
        key = (dst_name, in_port)
        incoming[key] = incoming.get(key, 0) + 1

    @staticmethod
    def _link_runtime_edges(
        src: Component, targets: Dict[str, List[Component]]
    ) -> None:
        """
        Populate src.next_components and dst.prev_components without duplicates.
        """
        seen: Set[str] = set()
        ordered_next: List[Component] = []
        for lst in targets.values():
            for dst in lst:
                if dst.name in seen:
                    continue
                ordered_next.append(dst)
                seen.add(dst.name)
                dst.prev_components.append(src)
        src.next_components = ordered_next

    def _enforce_contracts(self, incoming: Dict[Tuple[str, str], int]) -> None:
        """
        Enforce port contracts + schemas. Sources cannot have incoming edges.
        """
        for comp in self.components:
            # sources must not have incoming edges
            if getattr(comp, "ALLOW_NO_INPUTS", False):
                total_in = sum(
                    cnt for (tgt, _ip), cnt in incoming.items() if tgt == comp.name
                )
                if total_in:
                    raise ValueError(
                        f"Component {comp.name} is declared as a source "
                        f"(ALLOW_NO_INPUTS=True) but has {total_in} incoming edge(s)."
                    )

            used_out = self._collect_used_out(comp)
            used_in = self._collect_used_in(comp, incoming)

            out_specs = {
                p.name: p for p in getattr(comp, "expected_ports", lambda: [])()
            }
            in_specs = {
                p.name: p for p in getattr(comp, "expected_in_ports", lambda: [])()
            }

            self._check_out_contracts(comp, out_specs, used_out)
            self._check_in_contracts(comp, in_specs, used_in)
            self._check_schema_presence(comp, used_in, used_out)

    @staticmethod
    def _collect_used_out(comp: Component) -> Dict[str, int]:
        """
        Count fan-out per out port for a component.
        """
        result: Dict[str, int] = {}
        for p, dsts in getattr(comp, "out_routes", {}).items():
            result[p] = len(dsts)
        return result

    @staticmethod
    def _collect_used_in(
        comp: Component, incoming: Dict[Tuple[str, str], int]
    ) -> Dict[str, int]:
        """
        Count fan-in only on declared input ports.
        Sources (no inputs) have an empty dict and are validated elsewhere.
        """
        names = getattr(comp, "expected_in_port_names", lambda: [])()
        return {ip: incoming.get((comp.name, ip), 0) for ip in names}

    @staticmethod
    def _check_out_contracts(
        comp: Component, specs: Dict[str, Any], used: Dict[str, int]
    ) -> None:
        """
        Validate required/fanout on outputs.
        """
        for pname, spec in specs.items():
            n = used.get(pname, 0)
            if getattr(spec, "required", False) and n == 0:
                raise ValueError(
                    f"Component {comp.name}: required out port {pname!r} has no route"
                )
            if getattr(spec, "fanout", "many") == "one" and n > 1:
                raise ValueError(
                    f"Component {comp.name}: out port {pname!r} must have exactly "
                    f"one successor"
                )

    @staticmethod
    def _check_in_contracts(
        comp: Component, specs: Dict[str, Any], used: Dict[str, int]
    ) -> None:
        """Validate required/fanin on inputs."""
        for pname, spec in specs.items():
            n = used.get(pname, 0)
            if getattr(spec, "required", False) and n == 0:
                raise ValueError(
                    f"Component {comp.name}: required in port {pname!r} has "
                    f"no upstream edges"
                )
            if getattr(spec, "fanin", "many") == "one" and n != 1:
                raise ValueError(
                    f"Component {comp.name}: in port {pname!r} must have exactly "
                    f"one upstream edge"
                )

    @staticmethod
    def _check_schema_presence(
        comp: Component, used_in: Dict[str, int], used_out: Dict[str, int]
    ) -> None:
        """
        Ensure schemas exist for *used* ports.
        Prefer a component-provided helper if present.
        """
        ensure = getattr(comp, "ensure_schemas_for_used_ports", None)
        if callable(ensure):
            ensure(used_in_ports=used_in, used_out_ports=used_out)
            return

        # Fallback: look at common dict attributes
        out_schemas: Dict[str, object] = getattr(comp, "out_port_schemas", {})
        in_schemas: Dict[str, object] = getattr(comp, "in_port_schemas", {})
        for p, n in used_out.items():
            if n > 0 and p not in out_schemas:
                raise ValueError(f"Component {comp.name}: out port {p!r} has no schema")
        for p, n in used_in.items():
            if n > 0 and p not in in_schemas:
                raise ValueError(f"Component {comp.name}: in port {p!r} has no schema")

    @model_validator(mode="after")
    def _assign_strategies(self) -> Self:
        """
        After wiring, give every component the Job’s strategy.
        """
        for comp in self.components:
            # override whatever was on the component; use job-level strategy_type
            comp.strategy = get_strategy(self.strategy_type)

        return self

    @property
    def id(self) -> str:
        """
        Returns the unique identifier of the job.
        """
        return self._id


class JobExecution:
    """
    Runtime state for one execution of a JobDefinition.
    """

    def __init__(self, job: RuntimeJob, environment: Optional[Environment] = None):
        self._id: str = str(uuid4())
        self._job = job
        # each execution carries its own retry strategy
        self._retry_strategy = ConstantRetryStrategy(job.num_of_retries)
        self._max_attempts = job.num_of_retries + 1
        self._attempts: List[ExecutionAttempt] = []
        self._environment: Optional[Environment] = environment

        # each component gets its own sentinel instance
        self._sentinels: Dict[str, Sentinel] = {
            comp.id: Sentinel(comp.id) for comp in job.components
        }

    def start_attempt(self):
        if len(self._attempts) >= self._max_attempts:
            raise RuntimeError("No attempts left")
        attempt = ExecutionAttempt(len(self.attempts) + 1, self)
        self._attempts.append(attempt)

    def latest_attempt(self) -> "ExecutionAttempt":
        if not self._attempts:
            raise RuntimeError("No attempts have been started yet")
        return self._attempts[-1]

    @property
    def id(self) -> str:
        return self._id

    @property
    def job(self) -> RuntimeJob:
        return self._job

    @property
    def max_attempts(self) -> int:
        return self._max_attempts

    @property
    def attempts(self) -> List["ExecutionAttempt"]:
        return self._attempts

    @property
    def retry_strategy(self) -> RetryStrategy:
        """
        Returns the retry strategy for this job execution.
        """
        return self._retry_strategy

    @property
    def environment(self) -> Optional[Environment]:
        return self._environment

    @property
    def sentinels(self) -> Dict[str, Sentinel]:
        """
        Returns a mapping of component IDs to their sentinels.
        Sentinels are used to mark the end of a stream for each component.
        """
        return self._sentinels


class ExecutionAttempt:
    """
    Data for one try of a JobExecution.
    """

    def __init__(self, index: int, execution: JobExecution):
        if index < 1:
            raise ValueError("attempt number must be >= 1")
        self._id = str(uuid4())
        self._index = index
        self._error: str | None = None

        # runtime sets
        self._pending = {c.id for c in execution.job.components}
        self._succeeded = set()
        self._failed = set()
        self._cancelled = set()
        self._current_tasks: Dict[str, asyncio.Task] = {}

    @property
    def id(self) -> str:
        return self._id

    @property
    def index(self) -> int:
        return self._index

    @property
    def error(self) -> str | None:
        return self._error

    @error.setter
    def error(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("error must be a string")
        self._error = value

    @property
    def pending(self) -> set[str]:
        """
        Components that are still pending execution.
        """
        return self._pending

    @property
    def succeeded(self) -> set[str]:
        """
        Components that have successfully executed.
        """
        return self._succeeded

    @property
    def failed(self) -> set[str]:
        """
        Components that have failed execution.
        """
        return self._failed

    @property
    def cancelled(self) -> set[str]:
        """
        Components that have been cancelled.
        """
        return self._cancelled

    @property
    def current_tasks(self) -> Dict[str, asyncio.Task]:
        """
        Returns a mapping of component IDs to their current asyncio tasks.
        This is used to track the execution of components in the job.
        """
        return self._current_tasks

    @current_tasks.setter
    def current_tasks(self, tasks: Dict[str, asyncio.Task]) -> None:
        """
        Sets the current tasks for the job execution.
        This is used to track the execution of components in the job.
        """
        if not isinstance(tasks, dict):
            raise TypeError(
                "current_tasks must be a dictionary of component IDs to asyncio tasks"
            )
        self._current_tasks = tasks
