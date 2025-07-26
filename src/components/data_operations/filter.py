from src.components.data_operations.data_operations import DataOperationComponent
from typing import Any, Literal
from pydantic import Field
from src.components.registry import register_component
from src.components.dataclasses import Layout, MetaData
from src.components.base_component import get_strategy
from src.receivers.data_operations_receivers.filter_receiver import FilterReceiver


@register_component("filter")
class FilterComponent(DataOperationComponent):
    """
    Component for filtering data based on a condition.
    """

    type: Literal["filter"]
    filter_value: Any = Field(..., description="value filter is comparing against")
    operator: Literal["equals", "not_equals", "greater_than", "less_than"] = Field(
        ..., description="comparison operator for the filter"
    )

    @classmethod
    def build_objects(cls, values):
        """
        Build dependent objects for the stub component
        """
        values["layout"] = Layout(x_coord=values["x_coord"], y_coord=values["y_coord"])
        values["strategy"] = get_strategy(values["strategy_type"])
        values["receiver"] = FilterReceiver()
        values["metadata"] = MetaData(
            created_at=values["created_at"], created_by=values["created_by"]
        )

        return values

    def execute(self, data, **kwargs):
        """
        Execute the filter operation on the provided data.
        """
        pass
