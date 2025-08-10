from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.components.base_component import Component
from src.components.component_registry import register_component
from src.receivers.base_receiver import Receiver
from typing import Any, List, Dict
from pydantic import PrivateAttr


@register_component("test")
class StubComponent(Component):
    async def execute(self, data, metrics, **kwargs):
        metrics.lines_received = 1
        yield data

    def _build_objects(self) -> "StubComponent":
        """
        Build dependent objects for the stub component
        """
        self._receiver = StubReceiver()
        return self

    def process_row(
        self, row: dict[str, Any], metrics: "ComponentMetrics"
    ) -> dict[str, Any]:
        """
        placeholder method, not implemented in this stub component
        """

    def process_bulk(
        self, data: List[Dict[str, Any]], metrics: ComponentMetrics
    ) -> List[Dict[str, Any]]:
        """
        placeholder method, not implemented in this stub component
        """

    def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics) -> Any:
        """
        placeholder method, not implemented in this stub component
        """


@register_component("failtest")
class FailStubComponent(StubComponent):
    async def execute(self, data, metrics, **kwargs):
        if False:
            yield data
        raise RuntimeError("fail stubcomponent failed")

    def _build_objects(self) -> "FailStubComponent":
        """
        Build dependent objects for the failstub component
        """
        self._receiver = StubReceiver()
        return self


@register_component("stub_fail_once")
class StubFailOnce(Component):
    # a per‐instance flag, initialized to False
    _called: bool = PrivateAttr(default=False)

    def _build_objects(self) -> "StubFailOnce":
        self._receiver = StubReceiver()
        return self

    async def execute(self, data, metrics, **kwargs):
        if not self._called:
            self._called = True
            raise RuntimeError("fail first time")
        metrics.lines_received = 1
        yield "recovered"

    def process_row(
        self, row: dict[str, Any], metrics: "ComponentMetrics"
    ) -> dict[str, Any]:
        """
        placeholder method, not implemented in this stub component
        """

    def process_bulk(
        self, data: List[Dict[str, Any]], metrics: ComponentMetrics
    ) -> List[Dict[str, Any]]:
        """
        placeholder method, not implemented in this stub component
        """

    def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics) -> Any:
        """
        placeholder method, not implemented in this stub component
        """


class StubReceiver(Receiver):
    def execute(self, data, **kwargs):
        return data


@register_component("multi_source")
class MultiSource(Component):
    """
    Component that emits multiple rows in a streaming fashion.
    """

    count: int = 2
    _emitted: int = PrivateAttr(default=0)

    def _build_objects(self) -> "MultiSource":
        self._receiver = StubReceiver()
        return self

    async def execute(self, payload: Any, metrics, **kwargs):
        for i in range(self.count):
            metrics.lines_received = i + 1
            yield {"source": self.name, "index": i}
        # implicit return stops generator

    async def process_row(
        self, payload: Any, metrics: ComponentMetrics
    ) -> Dict[str, Any]:
        raise NotImplementedError

    async def process_bulk(self, data: Any, metrics: ComponentMetrics):
        raise NotImplementedError

    async def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics):
        raise NotImplementedError


@register_component("multi_echo")
class MultiEcho(Component):
    """
    Component that echoes each received row downstream.
    """

    def _build_objects(self) -> "MultiEcho":
        self._receiver = StubReceiver()
        return self

    async def execute(self, payload: Dict[str, Any], metrics, **kwargs):
        metrics.lines_received += 1
        yield self.receiver.execute(payload)

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> Dict[str, Any]:
        raise NotImplementedError

    async def process_bulk(self, data: Any, metrics: ComponentMetrics):
        raise NotImplementedError

    async def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics):
        raise NotImplementedError
