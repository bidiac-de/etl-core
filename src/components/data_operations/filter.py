from src.components.data_operations.data_operations import DataOperationComponent
from typing import Any, Literal, List, Dict
from pydantic import Field
from src.components.component_registry import register_component
from src.components.base_component import get_strategy
from src.receivers.data_operations_receivers.filter_receiver import FilterReceiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics


@register_component("filter")
class FilterComponent(DataOperationComponent):
    """
    Example class: Component for filtering data based on a condition

    """

    type: Literal["filter"]
    filter_value: Any = Field(..., description="value filter is comparing against")
    operator: Literal["equals", "not_equals", "greater_than", "less_than"] = Field(
        ..., description="comparison operator for the filter"
    )

    @classmethod
    def _build_objects(cls, values):
        """
        Build dependent objects for the stub component
        """
        values["strategy"] = get_strategy(values["strategy_type"])
        values["receiver"] = FilterReceiver()

        return values

    def process_row(
        self, row: dict[str, Any], metrics: "ComponentMetrics"
    ) -> dict[str, Any]:
        """
        call to the receiver method to process the row, needs to pass metrics,
        so receiver can log metrics
        """
        return self.receiver.process_row(row, self.filter_value, self.operator, metrics)

    def process_bulk(
        self, data: List[Dict[str, Any]], metrics: ComponentMetrics
    ) -> List[Dict[str, Any]]:
        """
        call to the receiver method to process the bulk data, needs to pass metrics,
        so receiver can log metrics
        """
        return self.receiver.process_bulk(
            data, self.filter_value, self.operator, metrics
        )

    def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics) -> Any:
        """
        call to the receiver method to process the big data, needs to pass metrics,
        so receiver can log metrics
        """
        return self.receiver.process_bigdata(
            chunk_iterable, self.filter_value, self.operator, metrics
        )
