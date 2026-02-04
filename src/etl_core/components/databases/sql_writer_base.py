from __future__ import annotations

from typing import Any, AsyncIterator, ClassVar, Dict

import dask.dataframe as dd
import pandas as pd

from etl_core.components.envelopes import Out
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.databases.sql_receiver import SQLReceiver


class SQLWriterBase:
    """Base class for SQL writers with shared row/bulk/bigdata logic."""

    receiver_class: ClassVar[type[SQLReceiver]]

    def _initialize_receiver(self) -> None:
        self._receiver = self.receiver_class()

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Write a single row and emit it (or receiver result) on 'out'."""
        result = await self._receiver.write_row(
            entity_name=self.entity_name,
            row=row,
            metrics=metrics,
            query=self._query,
            connection_handler=self.connection_handler,
        )
        yield Out(port="out", payload=result)

    async def process_bulk(
        self, data: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Write a pandas DataFrame and emit the same frame on 'out'."""
        result = await self._receiver.write_bulk(
            entity_name=self.entity_name,
            frame=data,
            metrics=metrics,
            query=self._query,
            connection_handler=self.connection_handler,
        )
        yield Out(port="out", payload=result)

    async def process_bigdata(
        self, ddf: dd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Write a Dask DataFrame and emit the same ddf on 'out'."""
        result = await self._receiver.write_bigdata(
            entity_name=self.entity_name,
            frame=ddf,
            metrics=metrics,
            query=self._query,
            connection_handler=self.connection_handler,
        )
        yield Out(port="out", payload=result)
