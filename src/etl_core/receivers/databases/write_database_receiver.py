from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict

import pandas as pd
from dask import dataframe as dd


class WriteDatabaseReceiver(ABC):
    """
    Generic, DB-agnostic write receiver.
    **driver_kwargs is a generic passthrough for engine-specific options.
    Generic implementations should ignore unknown keys.
    """

    @abstractmethod
    async def write_row(
        self,
        *,
        db: Any,
        entity_name: str,
        row: Dict[str, Any],
        metrics: Any,
        **driver_kwargs: Any,
    ) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def write_bulk(
        self,
        *,
        db: Any,
        entity_name: str,
        frame: pd.DataFrame,
        metrics: Any,
        chunk_size: int = 50_000,
        **driver_kwargs: Any,
    ) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    async def write_bigdata(
        self,
        *,
        db: Any,
        entity_name: str,
        frame: "dd.DataFrame",
        metrics: Any,
        partition_chunk_size: int = 50_000,
        **driver_kwargs: Any,
    ) -> "dd.DataFrame":
        raise NotImplementedError
