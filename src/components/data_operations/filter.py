from src.components.data_operations.data_operations import DataOperationComponent
from src.components.base_component import BaseComponentSchema
from typing import Any, Literal
from pydantic import Field


class FilterComponent(DataOperationComponent):
    """
    Component for filtering data based on a condition.
    """

    def execute(self, data, **kwargs):
        """
        Execute the filter operation on the provided data.
        """
        pass


class FilterComponentSchema(BaseComponentSchema):
    """
    Schema for FilterComponent
    """

    type: Literal["filter"]
    filter_value: Any = Field(..., description="value filter is comparing against")
    operator: Literal["equals", "not_equals", "greater_than", "less_than"] = Field(
        ..., description="comparison operator for the filter"
    )
