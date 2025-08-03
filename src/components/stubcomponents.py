from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.components.base_component import Component
from src.components.component_registry import register_component
from src.components.base_component import get_strategy
from src.receivers.base_receiver import Receiver
from typing import Any, List, Dict
from pydantic import PrivateAttr


@register_component("test")
class StubComponent(Component):
    def execute(self, data, metrics, **kwargs):
        metrics.lines_received = 1
        return data

    @classmethod
    def _build_objects(cls, values):
        """
        Build dependent objects for the stub component
        """
        values["strategy"] = get_strategy(values["strategy_type"])
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
    def execute(self, data, metrics: ComponentMetrics, **kwargs) -> Any:
        raise RuntimeError("fail stubcomponent failed")

    @classmethod
    def _build_objects(cls, values):
        """
        Build dependent objects for the failstub component
        """
        values["strategy"] = get_strategy(values["strategy_type"])
        values["receiver"] = StubReceiver()

        return values


@register_component("stub_fail_once")
class StubFailOnce(Component):
    # a per‐instance flag, initialized to False
    _called: bool = PrivateAttr(default=False)

    @classmethod
    def _build_objects(cls, values):
        values["strategy"] = get_strategy(values["strategy_type"])
        values["receiver"] = StubReceiver()
        return values

    def execute(self, data, metrics, **kwargs):
        # on the very first call of this instance, throw…
        if not self._called:
            self._called = True
            raise RuntimeError("fail first time")
        # …and on the retry, succeed:
        metrics.lines_received = 1
        return "recovered"

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
