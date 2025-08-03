from src.components.data_operations.data_operations import DataOperationsComponent
from typing import Any, List, Dict, Literal, TYPE_CHECKING
from pydantic import Field
from src.components.component_registry import register_component
from src.components.base_component import get_strategy
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.components.data_operations.comparison_rule import ComparisonRule

if TYPE_CHECKING:
    from src.receivers.data_operations_receivers.filter_receiver import FilterReceiver
    from src.metrics.component_metrics.component_metrics import ComponentMetrics


@register_component("filter")
class FilterComponent(DataOperationsComponent):
    """Component for filtering data based on a ComparisonRule."""

    type: Literal["filter"] = "filter"
    comparison_rule: ComparisonRule = Field(..., description="Rule defining the filter condition")

    @classmethod
    def build_objects(cls, values):
        from src.receivers.data_operations_receivers.filter_receiver import FilterReceiver
        values["strategy"] = get_strategy(values["strategy_type"])
        values["receiver"] = FilterReceiver()
        return values

    def process_row(self, row: Dict[str, Any], metrics: ComponentMetrics) -> Dict[str, Any]:
        """Process a single row with the filter rule."""
        return self.receiver.process_row(metrics, row, self.comparison_rule)

    def process_bulk(self, data: List[Dict[str, Any]], metrics: ComponentMetrics) -> List[Dict[str, Any]]:
        """Process multiple rows with the filter rule."""
        return self.receiver.process_bulk(metrics, data, self.comparison_rule)

    def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics) -> Any:
        """Process large datasets with the filter rule in a streaming fashion."""
        return self.receiver.process_bigdata(metrics, chunk_iterable, self.comparison_rule)