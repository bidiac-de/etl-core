from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List

import pandas as pd
from pydantic import ConfigDict, Field, model_validator

from src.components.file_components.file_component import FileComponent
from src.metrics.component_metrics.component_metrics import ComponentMetrics


class Excel(FileComponent, ABC):
    """
    Abstract base class for Excel file components.

    Contract:
    - process_row: async generator of dict rows
    - process_bulk: async generator yielding a pandas DataFrame once
    - process_bigdata: async generator yielding a big-data frame once
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

    sheet_name: str | int | None = Field(
        default=None,
        description="Excel sheet to read/write. If None, use library defaults.",
    )

    @model_validator(mode="after")
    def _validate_filepath(self) -> "Excel":
        """
        Validate filepath only for read-components. Writers may create new files.
        """
        path = Path(self.filepath)
        if str(self.comp_type).startswith("read_") and not path.exists():
            raise ValueError(f"Filepath does not exist: {path}")
        return self

    @abstractmethod
    async def process_row(
            self,
            row: Dict[str, Any],
            metrics: ComponentMetrics,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Stream one row at a time. For readers, the incoming `row` is ignored."""
        raise NotImplementedError

    @abstractmethod
    async def process_bulk(
            self,
            data: pd.DataFrame | List[Dict[str, Any]] | None,
            metrics: ComponentMetrics,
    ) -> AsyncIterator[pd.DataFrame]:
        """
        Yield a single pandas DataFrame. Readers ignore `data`, writers yield
        what they wrote (normalized to DataFrame if needed).
        """
        raise NotImplementedError

    @abstractmethod
    async def process_bigdata(
            self,
            chunk_iterable: Any,
            metrics: ComponentMetrics,
    ) -> AsyncIterator[Any]:
        """Yield a single big-data frame (e.g., dask.dataframe.DataFrame)."""
        raise NotImplementedError