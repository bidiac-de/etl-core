from src.components.base_component import Component
from src.components.registry import register_component


@register_component("test")
class TestComponent(Component):
    def execute(self, data, **kwargs):
        self.metrics = type("TestMetrics", (), {"lines_received": 1})()
        return data  # or return "test_output"


@register_component("failtest")
class FailTestComponent(TestComponent):
    def execute(self, data, **kwargs):
        raise RuntimeError("kaboom-chain")
