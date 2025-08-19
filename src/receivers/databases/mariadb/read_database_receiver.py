from abc import ABC, abstractmethod
from typing import AsyncIterator, Dict, Any
import pandas as pd
import dask.dataframe as dd

from src.receivers.base_receiver import Receiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.components.databases.sql_connection_handler import SQLConnectionHandler


class ReadDatabaseReceiver(Receiver, ABC):
    """Abstract receiver for reading data from databases (async + streaming)."""

    def __init__(self, connection_handler: SQLConnectionHandler):
        # Use object.__setattr__ to avoid Pydantic validation issues
        object.__setattr__(self, "_connection_handler", connection_handler)

    @property
    def connection_handler(self) -> SQLConnectionHandler:
        return self._connection_handler

    @abstractmethod
    async def read_row(
        self, query: str, params: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Yield single rows (as dicts) from a query.
        """
        pass

    @abstractmethod
    async def read_bulk(
        self, query: str, params: Dict[str, Any], metrics: ComponentMetrics
    ) -> pd.DataFrame:
        """
        Read 'bulk' data as a pandas DataFrame.
        """
        pass

    @abstractmethod
    async def read_bigdata(
        self, query: str, params: Dict[str, Any], metrics: ComponentMetrics
    ) -> dd.DataFrame:
        """
        Read 'big data' as a Dask DataFrame.
        """
        pass
