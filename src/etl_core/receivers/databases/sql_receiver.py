from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, AsyncIterator

import pandas as pd
import dask.dataframe as dd

from src.etl_core.receivers.databases.read_database_receiver import ReadDatabaseReceiver
from src.etl_core.receivers.databases.write_database_receiver import (
    WriteDatabaseReceiver,
)
from src.etl_core.components.databases.sql_connection_handler import (
    SQLConnectionHandler,
)


class SQLReceiver(ReadDatabaseReceiver, WriteDatabaseReceiver, ABC):
    """
    Base class for SQL database receivers (MariaDB, PostgreSQL, MySQL, etc.).

    This class provides SQL-specific functionality and abstracts away
    database-specific differences while maintaining the common interface.
    """

    def _get_connection(self, connection_handler: SQLConnectionHandler) -> Any:
        """Get the SQLAlchemy connection from the connection handler."""
        return connection_handler.lease().__enter__()

    @abstractmethod
    async def read_row(
        self,
        *,
        entity_name: str,
        metrics: Any,
        connection_handler: SQLConnectionHandler,
        batch_size: int = 1000,
        **driver_kwargs: Any,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Yield SQL rows as dictionaries from a query."""
        raise NotImplementedError

    @abstractmethod
    async def read_bulk(
        self,
        *,
        entity_name: str,
        metrics: Any,
        connection_handler: SQLConnectionHandler,
        **driver_kwargs: Any,
    ) -> pd.DataFrame:
        """Read query results as a pandas DataFrame."""
        raise NotImplementedError

    @abstractmethod
    async def read_bigdata(
        self,
        *,
        entity_name: str,
        metrics: Any,
        connection_handler: SQLConnectionHandler,
        **driver_kwargs: Any,
    ) -> dd.DataFrame:
        """Read large query results as a Dask DataFrame."""
        raise NotImplementedError

    @abstractmethod
    async def write_row(
        self,
        *,
        entity_name: str,
        row: Dict[str, Any],
        metrics: Any,
        connection_handler: SQLConnectionHandler,
        **driver_kwargs: Any,
    ) -> Dict[str, Any]:
        """Write a single row and return the result."""
        raise NotImplementedError

    @abstractmethod
    async def write_bulk(
        self,
        *,
        entity_name: str,
        frame: pd.DataFrame,
        metrics: Any,
        connection_handler: SQLConnectionHandler,
        **driver_kwargs: Any,
    ) -> pd.DataFrame:
        """Write a pandas DataFrame and return it."""
        raise NotImplementedError

    @abstractmethod
    async def write_bigdata(
        self,
        *,
        entity_name: str,
        frame: dd.DataFrame,
        metrics: Any,
        connection_handler: SQLConnectionHandler,
        **driver_kwargs: Any,
    ) -> dd.DataFrame:
        """Write a Dask DataFrame and return it."""
        raise NotImplementedError
