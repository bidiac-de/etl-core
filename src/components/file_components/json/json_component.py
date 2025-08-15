from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List
from pydantic import Field, ConfigDict

from src.components.file_components.file_component import FileComponent
from src.components.column_definition import ColumnDefinition
from src.metrics.component_metrics import ComponentMetrics


class JSON(FileComponent, ABC):
    """Abstract JSON component, async + streaming (yield)."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )

    @abstractmethod
    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Yield single rows (dict) from file.
        """
        raise NotImplementedError

    @abstractmethod
    async def process_bulk(
        self, data: List[Dict[str, Any]], metrics: ComponentMetrics
    ) -> AsyncIterator[Any]:
        """
        Yield pandas DataFrame chunks.
        """
        raise NotImplementedError

    @abstractmethod
    async def process_bigdata(
        self, chunk_iterable: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Any]:
        """
        Yield pandas DataFrame per (big) chunk/partition (e.g., from Dask).
        """
        raise NotImplementedError
