from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Dict
import pandas as pd
import dask.dataframe as dd

from etl_core.receivers.base_receiver import Receiver
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics


class DataOperationsReceiver(Receiver, ABC):
    """Async + streaming Receiver interface for data operations."""

    @abstractmethod
    async def process_row(
        self,
        rows: AsyncIterator[Dict[str, Any]],
        *,
        metrics: ComponentMetrics,
        **kwargs: Any,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Consume a stream of rows and yield filtered/transformed rows."""
        raise NotImplementedError

    @abstractmethod
    async def process_bulk(
        self,
        frames: AsyncIterator[pd.DataFrame],
        *,
        metrics: ComponentMetrics,
        **kwargs: Any,
    ) -> AsyncIterator[pd.DataFrame]:
        """Consume a stream of pandas DataFrame chunks and yield processed chunks."""
        raise NotImplementedError

    @abstractmethod
    async def process_bigdata(
        self,
        ddf: dd.DataFrame,
        *,
        metrics: ComponentMetrics,
        **kwargs: Any,
    ) -> dd.DataFrame:
        """Process a Dask DataFrame and return a Dask DataFrame."""
        raise NotImplementedError
