from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Dict, Optional

import pandas as pd
from pydantic import ConfigDict, Field

from src.components.file_components.file_component import FileComponent
from src.metrics.component_metrics.component_metrics import ComponentMetrics


class Excel(FileComponent, ABC):
    """Abstract base class for Excel file components."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )

    sheet_name: Optional[str] = Field(
        default="Sheet1", description="Default Excel sheet name"
    )

    @abstractmethod
    async def process_row(
            self,
            row: Dict[str, Any],
            metrics: ComponentMetrics,
            sheet_name: Optional[str] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Process one logical row. Implementations usually stream rows."""
        raise NotImplementedError

    @abstractmethod
    async def process_bulk(
            self,
            data: Optional[pd.DataFrame],
            metrics: ComponentMetrics,
            sheet_name: Optional[str] = None,
    ) -> AsyncIterator[pd.DataFrame]:
        """Process a whole DataFrame in one go."""
        raise NotImplementedError

    @abstractmethod
    async def process_bigdata(
            self,
            chunk_iterable: Any,
            metrics: ComponentMetrics,
            sheet_name: Optional[str] = None,
    ) -> AsyncIterator[Any]:
        """Process a large logical table (e.g., a Dask DataFrame)."""
        raise NotImplementedError