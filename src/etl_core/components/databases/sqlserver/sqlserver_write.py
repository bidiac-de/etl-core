from __future__ import annotations

from typing import Any, AsyncIterator, Dict

import dask.dataframe as dd
import pandas as pd
from pydantic import Field, model_validator

from etl_core.components.databases.sqlserver.sqlserver import SQLServerComponent
from etl_core.components.component_registry import register_component
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.databases.sqlserver.sqlserver_receiver import SQLServerReceiver
from etl_core.components.envelopes import Out


@register_component("write_sqlserver")
class SQLServerWrite(SQLServerComponent):
    """SQL Server writer supporting row, bulk, and bigdata modes."""

    params: Dict[str, Any] = Field(default_factory=dict, description="Query parameters")
    if_exists: str = Field(default="append", description="How to behave if the table already exists")

    @model_validator(mode="after")
    def _build_objects(self) -> "SQLServerWrite":
        self._receiver = SQLServerReceiver()
        return self

    async def process_row(
        self, payload: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Write rows one-by-one to SQL Server."""
        result = await self._receiver.write_row(
            entity_name=self.entity_name,
            metrics=metrics,
            data=payload,
            params=self.params,
            connection_handler=self.connection_handler,
        )
        yield Out(port="out", payload=result)

    async def process_bulk(
        self, payload: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Write pandas DataFrame to SQL Server."""
        if not isinstance(payload, pd.DataFrame):
            raise ValueError("Bulk mode requires pandas DataFrame input")
            
        result = await self._receiver.write_bulk(
            entity_name=self.entity_name,
            metrics=metrics,
            data=payload,
            params=self.params,
            if_exists=self.if_exists,
            connection_handler=self.connection_handler,
        )
        yield Out(port="out", payload=result)

    async def process_bigdata(
        self, payload: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Write Dask DataFrame to SQL Server in chunks."""
        if not isinstance(payload, dd.DataFrame):
            raise ValueError("Bigdata mode requires Dask DataFrame input")
            
        result = await self._receiver.write_bigdata(
            entity_name=self.entity_name,
            metrics=metrics,
            data=payload,
            params=self.params,
            if_exists=self.if_exists,
            connection_handler=self.connection_handler,
        )
        yield Out(port="out", payload=result)
