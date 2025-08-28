from __future__ import annotations

from typing import Any, AsyncIterator, Dict, ClassVar

import dask.dataframe as dd
import pandas as pd
from pydantic import Field, model_validator

from etl_core.components.databases.mariadb.mariadb import MariaDBComponent
from etl_core.components.component_registry import register_component
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.databases.mariadb.mariadb_receiver import MariaDBReceiver


@register_component("read_mariadb")
class MariaDBRead(MariaDBComponent):
    """MariaDB reader supporting row, bulk, and bigdata modes."""

    params: Dict[str, Any] = Field(default_factory=dict, description="Query parameters")

    @model_validator(mode="after")
    def _build_objects(self) -> "MariaDBRead":
        self._receiver = MariaDBReceiver()
        return self

    async def process_row(
        self, payload: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Stream rows one-by-one and envelope them for routing."""
        async for row in self._receiver.read_row(
            entity_name=self.entity_name,
            metrics=metrics,
            query=self.query,
            params=self.params,
            connection_handler=self.connection_handler,
        ):
            yield Out(port="out", payload=row)

    async def process_bulk(
        self, payload: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Read full result as a pandas DataFrame and envelope it."""
        frame: pd.DataFrame = await self._receiver.read_bulk(
            entity_name=self.entity_name,
            metrics=metrics,
            query=self.query,
            params=self.params,
            connection_handler=self.connection_handler,
        )
        yield Out(port="out", payload=frame)

    async def process_bigdata(
        self, payload: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Read result as a Dask DataFrame and envelope it."""
        ddf: dd.DataFrame = await self._receiver.read_bigdata(
            entity_name=self.entity_name,
            metrics=metrics,
            query=self.query,
            params=self.params,
            connection_handler=self.connection_handler,
        )
        yield Out(port="out", payload=ddf)
