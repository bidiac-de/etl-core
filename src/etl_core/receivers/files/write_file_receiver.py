from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict
import pandas as pd
import dask.dataframe as dd

from etl_core.receivers.base_receiver import Receiver
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics


class WriteFileReceiver(Receiver, ABC):
    """Abstract receiver for writing data (async + streaming-friendly)."""

    @abstractmethod
    async def write_row(
        self, filepath: Path, metrics: ComponentMetrics, row: Dict[str, Any]
    ) -> None:
        """
        Write a single row.
        """
        pass

    @abstractmethod
    async def write_bulk(
        self,
        filepath: Path,
        metrics: ComponentMetrics,
        data: pd.DataFrame,
    ) -> None:
        """
        Write multiple rows at once.
        Accepts a pandas DataFrame or a list of dicts.
        """
        pass

    @abstractmethod
    async def write_bigdata(
        self, filepath: Path, metrics: ComponentMetrics, data: dd.DataFrame
    ) -> None:
        """
        Write large datasets (e.g., Dask DataFrame partitions).
        """
        pass
