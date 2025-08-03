from abc import ABC, abstractmethod
from typing import Any, Dict, List
from src.metrics.component_metrics.component_metrics import ComponentMetrics


class DataOperationsReceiver(ABC):
    """Abstract base receiver for data operation components."""

    @abstractmethod
    def process_row(self, row: Dict[str, Any], *args, metrics: ComponentMetrics, **kwargs) -> Dict[str, Any]:
        """Process a single row."""
        raise NotImplementedError

    @abstractmethod
    def process_bulk(self, data: List[Dict[str, Any]], *args, metrics: ComponentMetrics, **kwargs) -> List[Dict[str, Any]]:
        """Process multiple rows."""
        raise NotImplementedError

    @abstractmethod
    def process_bigdata(self, chunk_iterable: Any, *args, metrics: ComponentMetrics, **kwargs) -> Any:
        """Process large datasets in a streaming/big data fashion."""
        raise NotImplementedError