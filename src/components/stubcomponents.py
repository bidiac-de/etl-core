from __future__ import annotations

from typing import Any, AsyncIterator, Dict, ClassVar

from pydantic import PrivateAttr

from src.components.base_component import Component
from src.components.component_registry import register_component
from src.components.envelopes import Out
from src.components.wiring.ports import InPortSpec, OutPortSpec
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.base_receiver import Receiver

import pandas as pd
import dask.dataframe as dd


class StubReceiver(Receiver):
    def execute(self, data: Any, **kwargs: Any) -> Any:
        # trivial pass-through
        return data


@register_component("test")
class StubComponent(Component):
    """
    Basic echo component with one explicit input and one explicit output.
    Declares per-port schemas via `in_schema` / `out_schema` mapped during build.
    """

    # Explicit ports: no implicit single-input
    INPUT_PORTS = (InPortSpec(name="in"),)
    OUTPUT_PORTS = (OutPortSpec(name="out"),)

    def _build_objects(self) -> "StubComponent":
        """Wire a trivial receiver and map schemas onto declared ports."""
        self._receiver = StubReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any] | Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """
        Echo a single row and bump the counter. Always yield an Out envelope.
        """
        metrics.lines_received += 1
        payload: Dict[str, Any] = row if isinstance(row, dict) else {"value": row}
        yield Out("out", payload)

    async def process_bulk(
        self, data: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """
        Trivial bulk implementation so BulkExecutionStrategy's envelope check passes.
        """
        # no special metrics in these stubs; they are exercised in row mode mostly
        yield Out("out", data)

    async def process_bigdata(
        self, chunk_iterable: dd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """
        Trivial bigdata implementation so BigDataExecutionStrategy's
        envelope check passes.
        """
        yield Out("out", chunk_iterable)


@register_component("failtest")
class FailStubComponent(StubComponent):
    """
    Fails immediately while preserving async-generator
    semantics so strategies can `async for`.
    """

    def _build_objects(self) -> "FailStubComponent":
        self._receiver = StubReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any] | Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        # keep generator type (no actual yield executed)
        for _ in ():
            yield Out("out", {})
        raise RuntimeError("fail stubcomponent failed")


@register_component("stub_fail_once")
class StubFailOnce(StubComponent):
    """
    Fails the first time, succeeds on the next try.
    """

    _called: bool = PrivateAttr(default=False)

    def _build_objects(self) -> "StubFailOnce":
        self._receiver = StubReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any] | Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        if not self._called:
            self._called = True
            for _ in ():
                yield Out("out", {})  # pragma: no cover
            raise RuntimeError("fail first time")

        metrics.lines_received += 1
        yield Out("out", {"recovered": True})


@register_component("multi_source")
class MultiSource(StubComponent):
    """
    True source component: no input ports, only an output.
    Emits `count` rows.
    """

    # Source: explicitly no inputs, and must opt-in to allow this.
    INPUT_PORTS = ()
    ALLOW_NO_INPUTS: ClassVar[bool] = True
    OUTPUT_PORTS = (OutPortSpec(name="out"),)

    count: int = 2

    def _build_objects(self) -> "MultiSource":
        self._receiver = StubReceiver()
        return self

    async def process_row(
        self, payload: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        for i in range(self.count):
            metrics.lines_received = i + 1
            yield Out("out", {"source": self.name, "index": i})

    async def process_bulk(
        self, data: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        yield Out("out", data)

    async def process_bigdata(
        self, chunk_iterable: dd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        yield Out("out", chunk_iterable)


@register_component("multi_echo")
class MultiEcho(StubComponent):
    """
    Echoes each received row downstream using the receiver.
    """

    def _build_objects(self) -> "MultiEcho":
        self._receiver = StubReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any] | Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        metrics.lines_received += 1
        payload: Dict[str, Any] = row if isinstance(row, dict) else {"value": row}
        echoed = self.receiver.execute(payload)
        yield Out("out", echoed)

    async def process_bulk(
        self, data: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        yield Out("out", data)

    async def process_bigdata(
        self, chunk_iterable: dd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        yield Out("out", chunk_iterable)


@register_component("test_source_dynamic_ports")
class TestSource(Component):
    """
    Simple source that emits a few rows.
    Declares a single out port "out". No inputs.
    """

    INPUT_PORTS: ClassVar[tuple[InPortSpec, ...]] = ()
    OUTPUT_PORTS: ClassVar[tuple[OutPortSpec, ...]] = (OutPortSpec(name="out"),)
    ALLOW_NO_INPUTS: ClassVar[bool] = True

    count: int = 3

    def _build_objects(self) -> "TestSource":
        return self

    def ensure_schemas_for_used_ports(
        self,
        used_in_ports: Dict[str, int],
        used_out_ports: Dict[str, int],
    ) -> None:
        # tests focus on port behavior; skip schema enforcement
        return

    async def process_row(
        self, payload: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        for i in range(self.count):
            metrics.lines_received = i + 1
            status = "ok" if i % 2 == 0 else "err"
            yield Out("out", {"i": i, "status": status})

    async def process_bulk(self, *args: Any, **kwargs: Any) -> AsyncIterator[Out]:
        for _ in ():
            yield Out("out", None)

    async def process_bigdata(self, *args: Any, **kwargs: Any) -> AsyncIterator[Out]:
        for _ in ():
            yield Out("out_bigdata", None)


@register_component("test_router_dynamic_ports")
class TestRouter(Component):
    """
    Router that sends row to Out(row['status'], row).
    - Has a single input port "in".
    - OUTPUT ports are provided dynamically via extra_output_ports in config.
    """

    INPUT_PORTS: ClassVar[tuple[InPortSpec, ...]] = (InPortSpec(name="in"),)
    # OUTPUT_PORTS intentionally not set as class-level; extra_output_ports is used.

    def _build_objects(self) -> "TestRouter":
        return self

    def ensure_schemas_for_used_ports(
        self,
        used_in_ports: Dict[str, int],
        used_out_ports: Dict[str, int],
    ) -> None:
        return

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        metrics.lines_received += 1
        port = row.get("status", "default")
        yield Out(str(port), row)

    async def process_bulk(self, *args: Any, **kwargs: Any) -> AsyncIterator[Out]:
        for _ in ():
            yield Out("unused_bulk", None)

    async def process_bigdata(self, *args: Any, **kwargs: Any) -> AsyncIterator[Out]:
        for _ in ():
            yield Out("unused_bigdata", None)


@register_component("test_sink_dynamic_ports")
class TestSink(Component):
    """
    Sink with a single input port "in". No outputs needed for these tests.
    """

    INPUT_PORTS: ClassVar[tuple[InPortSpec, ...]] = (InPortSpec(name="in"),)
    OUTPUT_PORTS: ClassVar[tuple[OutPortSpec, ...]] = ()

    def _build_objects(self) -> "TestSink":
        return self

    def ensure_schemas_for_used_ports(
        self,
        used_in_ports: Dict[str, int],
        used_out_ports: Dict[str, int],
    ) -> None:
        return

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        metrics.lines_received += 1
        for _ in ():
            yield Out("unused", None)

    async def process_bulk(self, *args: Any, **kwargs: Any) -> AsyncIterator[Out]:
        for _ in ():
            yield Out("unused_bulk", None)

    async def process_bigdata(self, *args: Any, **kwargs: Any) -> AsyncIterator[Out]:
        for _ in ():
            yield Out("unused_bigdata", None)


@register_component("test_merge_dynamic_inputs")
class TestMerge(Component):
    """
    Component with dynamic input ports (extra_input_ports).
    It just accepts inputs and yields nothing.
    """

    INPUT_PORTS: ClassVar[tuple[InPortSpec, ...]] = ()
    OUTPUT_PORTS: ClassVar[tuple[OutPortSpec, ...]] = ()

    def _build_objects(self) -> "TestMerge":
        return self

    def ensure_schemas_for_used_ports(  # type: ignore[override]
        self,
        used_in_ports: Dict[str, int],
        used_out_ports: Dict[str, int],
    ) -> None:
        return

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        metrics.lines_received += 1
        for _ in ():
            yield Out("unused", None)

    async def process_bulk(self, *args: Any, **kwargs: Any) -> AsyncIterator[Out]:
        for _ in ():
            yield Out("unused_bulk", None)

    async def process_bigdata(self, *args: Any, **kwargs: Any) -> AsyncIterator[Out]:
        for _ in ():
            yield Out("unused_bigdata", None)
