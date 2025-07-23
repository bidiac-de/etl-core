from src.components.data_operations.data_operations import DataOperationComponent
from typing import Any, Literal
from pydantic import Field
from src.components.registry import register_component


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

    def execute(self, data, **kwargs):
        """
        Execute the filter operation on the provided data.
        """
        pass
