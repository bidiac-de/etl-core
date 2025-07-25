from src.components.base_component import Component
from src.components.registry import register_component


@register_component("test")
class StubComponent(Component):
    def execute(self, data, **kwargs):
        self.metrics = type("TestMetrics", (), {"lines_received": 1})()
        return data


@register_component("failtest")
class FailStubComponent(StubComponent):
    def execute(self, data, **kwargs):
        raise RuntimeError("kaboom-chain")


@register_component("stub_fail_once")
class StubFailOnce(Component):
    _called = False

    def execute(self, data, **kwargs):
        if not StubFailOnce._called:
            StubFailOnce._called = True
            raise RuntimeError("fail first time")
        self.metrics = type("M", (), {"lines_received": 2})()
        return "recovered"
