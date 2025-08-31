from __future__ import annotations

from typing import Any, Dict, AsyncIterator

import pandas as pd
import dask.dataframe as dd
from pydantic import Field, model_validator

from etl_core.components.databases.sqlserver.sqlserver import SQLServerComponent
from etl_core.components.component_registry import register_component
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.databases.sqlserver.sqlserver_receiver import SQLServerReceiver
from etl_core.components.envelopes import Out
from etl_core.components.wiring.ports import OutPortSpec


@register_component("read_sqlserver")
class SQLServerRead(SQLServerComponent):
    """SQL Server reader supporting row, bulk, and bigdata modes."""

    OUTPUT_PORTS = (OutPortSpec(name="out", required=True, fanout="many"),)

    ALLOW_NO_INPUTS = True

    query: str = Field(default="", description="SQL query for read operations")
    params: Dict[str, Any] = Field(default_factory=dict, description="Query parameters")

    @model_validator(mode="after")
    def _build_objects(self) -> "SQLServerRead":
        """Build objects after validation."""
        self._receiver = SQLServerReceiver()
        return self

    async def process_row(
        self, payload: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Stream rows one-by-one and envelope them for routing."""
        async for result in self._receiver.read_row(
            entity_name=self.entity_name,
            metrics=metrics,
            query=self.query,
            params=self.params,
            connection_handler=self.connection_handler,
        ):
            yield Out(port="out", payload=result)

    async def process_bulk(
        self,
        payload: Any,
        metrics: ComponentMetrics,
    ) -> AsyncIterator[Out]:
        """Read query results as a pandas DataFrame."""
        frame: pd.DataFrame = await self._receiver.read_bulk(
            entity_name=self.entity_name,
            metrics=metrics,
            connection_handler=self.connection_handler,
            query=self.query,
            params=self.params,
        )
        yield Out(port="out", payload=frame)

    async def process_bigdata(
        self, payload: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Read large query results as a Dask DataFrame."""
        ddf: dd.DataFrame = await self._receiver.read_bigdata(
            entity_name=self.entity_name,
            metrics=metrics,
            connection_handler=self.connection_handler,
            query=self.query,
            params=self.params,
        )
        yield Out(port="out", payload=ddf)
