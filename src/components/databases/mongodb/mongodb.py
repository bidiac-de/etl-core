from src.components.databases.database import DatabaseComponent
from src.components.base_component import Component
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import pandas as pd
from src.metrics.component_metrics.component_metrics import ComponentMetrics


class MongoDBComponent(Component, ABC):
    """
    MongoDB component for orchestrating MongoDB operations.
    """

    @abstractmethod
    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> Dict[str, Any]:
        """
        Process a single entry from a MongoDB collection.
        """
        raise NotImplementedError

    @abstractmethod
    async def process_bulk(
        self, data: Optional[pd.DataFrame], metrics: ComponentMetrics
    ) -> List[Dict[str, Any]]:
        """
        Process multiple entrys from a MongoDB collection as a bulk operation.
        """
        raise NotImplementedError

    @abstractmethod
    async def process_bigdata(
        self, chunk_iterable: Any, metrics: ComponentMetrics
    ) -> Any:
        """
        Process MongoDB data with a framework suitable for big data operations.
        """
        raise NotImplementedError
