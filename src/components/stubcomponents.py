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

    @classmethod
    def _build_objects(cls, values):
        """
        Build dependent objects for the stub component
        """
        values["receiver"] = StubReceiver()

        return values

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

    @classmethod
    def _build_objects(cls, values):
        """
        Build dependent objects for the failstub component
        """
        values["receiver"] = StubReceiver()

        return values


@register_component("stub_fail_once")
class StubFailOnce(Component):
    # a per‐instance flag, initialized to False
    _called: bool = PrivateAttr(default=False)

    @classmethod
    def _build_objects(cls, values):
        values["receiver"] = StubReceiver()
        return values

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
