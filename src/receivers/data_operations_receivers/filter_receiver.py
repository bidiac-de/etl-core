from typing import Any, Dict, List, Generator, Union
from src.metrics.component_metrics.data_operations_metrics.filter_metrics import FilterMetrics
from src.receivers.data_operations_receivers.data_operations_receiver import DataOperationsReceiver
from src.utils.data_operations_helper import DataOperationsHelper
from src.components.data_operations.comparison_rule import ComparisonRule


class FilterReceiver(DataOperationsReceiver):
    """Receiver for filtering rows of data using a ComparisonRule."""

    def process_row(self, metrics: FilterMetrics, row: Dict[str, Any], rule: ComparisonRule) -> Dict[str, Any]:
        metrics.lines_received += 1
        if DataOperationsHelper.matches(row, rule):
            metrics.lines_forwarded += 1
            return row
        metrics.lines_dismissed += 1
        return {}

    def process_bulk(self, metrics: FilterMetrics, data: List[Dict[str, Any]], rule: ComparisonRule) -> List[Dict[str, Any]]:
        result = []
        for row in data:
            processed_row = self.process_row(metrics, row, rule)
            if processed_row:
                result.append(processed_row)
        return result

    def process_bigdata(
            self,
            metrics: FilterMetrics,
            chunk_iterable: Union[Generator[Dict[str, Any], None, None], Any],
            rule: ComparisonRule,
    ) -> Generator[Dict[str, Any], None, None]:
        for chunk in chunk_iterable:
            for row in chunk:
                processed_row = self.process_row(metrics, row, rule)
                if processed_row:
                    yield processed_row