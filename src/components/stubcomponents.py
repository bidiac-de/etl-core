from src.metrics.component_metrics import ComponentMetrics
from src.components.base_component import Component
from src.components.registry import register_component
from src.components.dataclasses import Layout, MetaData
from src.components.base_component import get_strategy
from src.receivers.base_receiver import Receiver
from datetime import datetime


@register_component("test")
class StubComponent(Component):
    def execute(self, data, **kwargs):
        self.create_metric_object()
        self.metrics.lines_received = 1
        return data

    def create_metric_object(self):
        """
        Create a fresh metrics object for the component
        """
        self.metrics = ComponentMetrics(
            started_at=datetime.now(),
            processing_time=0,
            error_count=0,
            lines_received=0,
            lines_forwarded=0,
        )

    @classmethod
    def build_objects(cls, values):
        """
        Build dependent objects for the stub component
        """
        values["layout"] = Layout(x_coord=values["x_coord"], y_coord=values["y_coord"])
        values["strategy"] = get_strategy(values["strategy_type"])
        values["receiver"] = StubReceiver()
        values["metadata"] = MetaData(
            created_at=values["created_at"], created_by=values["created_by"]
        )

        return values


@register_component("failtest")
class FailStubComponent(StubComponent):
    def execute(self, data, **kwargs):
        self.create_metric_object()
        raise RuntimeError("fail stubcomponent failed")

    @classmethod
    def build_objects(cls, values):
        """
        Build dependent objects for the failstub component
        """
        values["layout"] = Layout(x_coord=values["x_coord"], y_coord=values["y_coord"])
        values["strategy"] = get_strategy(values["strategy_type"])
        values["receiver"] = StubReceiver()
        values["metadata"] = MetaData(
            created_at=values["created_at"], created_by=values["created_by"]
        )

        return values


@register_component("stub_fail_once")
class StubFailOnce(Component):
    _called = False

    def execute(self, data, **kwargs):
        self.create_metric_object()
        if not StubFailOnce._called:
            StubFailOnce._called = True
            raise RuntimeError("fail first time")
        self.metrics.lines_received = 1
        return "recovered"

    def create_metric_object(self):
        """
        Create a fresh metrics object for the component
        """
        self.metrics = ComponentMetrics(
            started_at=datetime.now(),
            processing_time=0,
            error_count=0,
            lines_received=0,
            lines_forwarded=0,
        )

    @classmethod
    def build_objects(cls, values):
        """
        Build dependent objects for the stub component
        """
        values["layout"] = Layout(x_coord=values["x_coord"], y_coord=values["y_coord"])
        values["strategy"] = get_strategy(values["strategy_type"])
        values["receiver"] = StubReceiver()
        values["metadata"] = MetaData(
            created_at=values["created_at"], created_by=values["created_by"]
        )

        return values


class StubReceiver(Receiver):
    def execute(self, data, **kwargs):
        return data
