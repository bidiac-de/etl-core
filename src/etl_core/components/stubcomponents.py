from __future__ import annotations

from typing import Any, AsyncIterator, Dict, List

from pydantic import PrivateAttr

from etl_core.components.base_component import Component
from etl_core.components.component_registry import register_component
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.base_receiver import Receiver


@register_component("test", hidden=True)
class StubComponent(Component):

    def _build_objects(self) -> "StubComponent":
        """Wire a trivial receiver."""
        self._receiver = StubReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any] | Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Echo a single row and bump the counter.
        Works for root components (None payload) and dict payloads alike.
        """
        metrics.lines_received += 1
        out: Dict[str, Any] = row if isinstance(row, dict) else {"value": row}
        yield out

    async def process_bulk(
        self, data: List[Dict[str, Any]], metrics: ComponentMetrics
    ) -> List[Dict[str, Any]]:
        metrics.lines_received += len(data)
        return data

    async def process_bigdata(
        self, chunk_iterable: Any, metrics: ComponentMetrics
    ) -> Any:
        return chunk_iterable


@register_component("failtest", hidden=True)
class FailStubComponent(StubComponent):
    def _build_objects(self) -> "FailStubComponent":
        """Wire a trivial receiver."""
        self._receiver = StubReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any] | Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Fail immediately on first iteration while still conforming to the
        async-iterator contract (so the strategy can `async for` cleanly).
        """
        # keep generator semantics without unreachable-after-raise code
        for _ in ():  # no iterations; presence of `yield` makes this an async-gen
            # this branch never runs, but keeps the function an async generator
            yield {}  # noqa: B901 (intentional: establish generator type)
        raise RuntimeError("fail stubcomponent failed")


@register_component("stub_fail_once", hidden=True)
class StubFailOnce(Component):
    """
    Fails the first time, succeeds on the next try.
    """
    _called: bool = PrivateAttr(default=False)

    def _build_objects(self) -> "StubFailOnce":
        self._receiver = StubReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any] | Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        First invocation raises; subsequent invocations yield a single
        'recovered' row and increment metrics.
        """
        if not self._called:
            self._called = True
            # same pattern as above to preserve async-generator contract
            for _ in ():
                yield {}
            raise RuntimeError("fail first time")

        metrics.lines_received += 1
        yield {"recovered": True}

    async def process_bulk(
        self, data: List[Dict[str, Any]], metrics: ComponentMetrics
    ) -> List[Dict[str, Any]]:
        return data

    async def process_bigdata(
        self, chunk_iterable: Any, metrics: ComponentMetrics
    ) -> Any:
        return chunk_iterable


class StubReceiver(Receiver):
    def execute(self, data, **kwargs):
        return data


@register_component("multi_source", hidden=True)
class MultiSource(Component):
    """
    Emits multiple rows in a streaming fashion; used by pipeline tests.
    """
    count: int = 2
    _emitted: int = PrivateAttr(default=0)

    def _build_objects(self) -> "MultiSource":
        self._receiver = StubReceiver()
        return self

    async def process_row(
        self, payload: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Generate `count` rows regardless of input payload and update metrics
        progressively so the final value equals `count`.
        """
        for i in range(self.count):
            metrics.lines_received = i + 1
            yield {"source": self.name, "index": i}

    async def process_bulk(self, data: Any, metrics: ComponentMetrics) -> Any:
        raise NotImplementedError

    async def process_bigdata(
        self, chunk_iterable: Any, metrics: ComponentMetrics
    ) -> Any:
        raise NotImplementedError


@register_component("multi_echo", hidden=True)
class MultiEcho(Component):
    """
    Echoes each received row downstream; used by pipeline tests.
    """

    def _build_objects(self) -> "MultiEcho":
        self._receiver = StubReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any] | Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Echo each row, bump the counter, and stream it out.
        """
        metrics.lines_received += 1
        out: Dict[str, Any] = row if isinstance(row, dict) else {"value": row}
        echoed = self.receiver.execute(out)
        yield echoed

    async def process_bulk(self, data: Any, metrics: ComponentMetrics) -> Any:
        raise NotImplementedError

    async def process_bigdata(
        self, chunk_iterable: Any, metrics: ComponentMetrics
    ) -> Any:
        raise NotImplementedError
