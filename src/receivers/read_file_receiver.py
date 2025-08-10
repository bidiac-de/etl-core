from abc import ABC, abstractmethod
from typing import AsyncIterator, Dict, Any

from src.receivers.base_receiver import Receiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics

class ReadFileReceiver(Receiver, ABC):
    """Abstract receiver for reading data (async + streaming)."""

    def __init__(self):
        super().__init__()

    @abstractmethod
    async def read_row(self, metrics: ComponentMetrics) -> AsyncIterator[Dict[str, Any]]:
        """
        Yield single rows (as dicts).
        """
        pass

    @abstractmethod
    async def read_bulk(self, metrics: ComponentMetrics):
        """
        Read 'bulk' data.
        EITHER yield pd.DataFrame chunks (AsyncIterator[pd.DataFrame])
        OR return a single pd.DataFrame (awaitable).
        Pick one shape and keep it consistent across receivers.
        """
        pass

    @abstractmethod
    async def read_bigdata(self, metrics: ComponentMetrics):
        """
        Read 'big data' (usually Dask).
        EITHER return a dd.DataFrame (awaitable)
        OR yield dd.DataFrame partitions.
        """
        pass