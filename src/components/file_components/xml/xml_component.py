from __future__ import annotations
from typing import Any, AsyncIterator, Dict, List
from pydantic import Field, ConfigDict
from abc import ABC, abstractmethod

from src.components.file_components.file_component import FileComponent
from src.metrics.component_metrics.component_metrics import ComponentMetrics

class XML(FileComponent, ABC):
    """Abstract XML component, async + streaming (yield) – analogous to JSON."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )

    root_tag: str = Field(default="rows")
    record_tag: str = Field(default="row")

    @abstractmethod
    async def process_row(
            self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncIterator[Dict[str, Any]]:
        """Yield individual records (dict) from an XML file."""
        raise NotImplementedError

    @abstractmethod
    async def process_bulk(
            self, data: List[Dict[str, Any]] | Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Any]:
        """Yield pandas DataFrame chunks (usually a single chunk)."""
        raise NotImplementedError

    @abstractmethod
    async def process_bigdata(
            self, chunk_iterable: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Any]:
        """Yield a DataFrame for each (large) chunk/partition (e.g. Dask)."""
        raise NotImplementedError