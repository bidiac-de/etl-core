from abc import ABC
from typing import Any, Dict, List, Optional, Literal
from pydantic import Field, ConfigDict

from src.components.data_operations.data_operations import DataOperationsComponent
from src.components.data_operations.comparison_rule import ComparisonRule
from src.components.base_component import get_strategy
from src.receivers.data_operations_receivers.filter_receiver import FilterReceiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics


class FilterComponent(DataOperationsComponent, ABC):
    """Component for filtering data based on a ComparisonRule."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )

    type: Literal["filter"] = "filter"
    comparison_rule: ComparisonRule = Field(..., description="Rule defining the filter condition")
    receiver: Optional[FilterReceiver] = Field(default=None)

    def process_row(self, row: Dict[str, Any], metrics: ComponentMetrics) -> Optional[Dict[str, Any]]:
        if self.receiver is None:
            raise ValueError("Receiver is not set for row processing")
        return self.receiver.process_row(metrics, row, self.comparison_rule)

    def process_bulk(self, data: List[Dict[str, Any]], metrics: ComponentMetrics) -> List[Dict[str, Any]]:
        if self.receiver is None:
            raise ValueError("Receiver is not set for bulk processing")
        return self.receiver.process_bulk(metrics, data, self.comparison_rule)

    def process_bigdata(self, chunk_iterable: Any, metrics: ComponentMetrics) -> Any:
        if self.receiver is None:
            raise ValueError("Receiver is not set for bigdata processing")
        return self.receiver.process_bigdata(metrics, chunk_iterable, self.comparison_rule)

    @classmethod
    def build_objects(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values["strategy"] = get_strategy(values["strategy_type"])
        values["receiver"] = FilterReceiver()
        return values