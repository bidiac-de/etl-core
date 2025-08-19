from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Dict

import dask.dataframe as dd
import pandas as pd

from src.etl_core.receivers.base_receiver import Receiver


class ReadDatabaseReceiver(Receiver, ABC):
    """
    DB-agnostic read receiver supporting row/bulk/bigdata.

    Contracts:
      - read_row:     async iterator of dicts (true streaming)
      - read_bulk:    pandas.DataFrame of all rows
      - read_bigdata: dask.dataframe.DataFrame built lazily from chunks

    **driver_kwargs is a generic passthrough for engine-specific options.
    Generic receivers MUST ignore unknown keys.
    """

    @abstractmethod
    async def read_row(
        self,
        *,
        db: Any,
        entity_name: str,
        metrics: Any,
        batch_size: int = 1000,
        **driver_kwargs: Any,
    ) -> AsyncIterator[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    async def read_bulk(
        self,
        *,
        db: Any,
        entity_name: str,
        metrics: Any,
        **driver_kwargs: Any,
    ) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    async def read_bigdata(
        self,
        *,
        db: Any,
        entity_name: str,
        metrics: Any,
        chunk_size: int = 100_000,
        **driver_kwargs: Any,
    ) -> dd.DataFrame:
        raise NotImplementedError
