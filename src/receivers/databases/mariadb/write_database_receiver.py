from abc import ABC, abstractmethod
from typing import Any, Dict, List, Union
import pandas as pd
import dask.dataframe as dd

from src.receivers.base_receiver import Receiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.components.databases.connection_handler import ConnectionHandler


class WriteDatabaseReceiver(Receiver, ABC):
    """Abstract receiver for writing data to databases (async + streaming-friendly)."""

    def __init__(self, connection_handler: ConnectionHandler):
        # Use object.__setattr__ to avoid Pydantic validation issues
        object.__setattr__(self, '_connection_handler', connection_handler)

    @property
    def connection_handler(self) -> ConnectionHandler:
        return self._connection_handler

    @abstractmethod
    async def write_row(
            self,
            table: str,
            data: Dict[str, Any],
            metrics: ComponentMetrics
    ) -> None:
        """
        Write a single row to a table.
        """
        pass

    @abstractmethod
    async def write_bulk(
            self,
            table: str,
            data: Union[pd.DataFrame, List[Dict[str, Any]]],
            metrics: ComponentMetrics,
    ) -> None:
        """
        Write multiple rows at once.
        Accepts a pandas DataFrame or a list of dicts.
        """
        pass

    @abstractmethod
    async def write_bigdata(
            self,
            table: str,
            metrics: ComponentMetrics,
            data: dd.DataFrame
    ) -> None:
        """
        Write large datasets (e.g., Dask DataFrame partitions).
        """
        pass
