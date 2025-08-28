from typing import Any, Dict, AsyncIterator
import pandas as pd
import dask.dataframe as dd
from pydantic import Field, model_validator

from etl_core.components.databases.postgresql.postgresql import PostgreSQLComponent
from etl_core.components.component_registry import register_component
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.databases.postgresql.postgresql_receiver import PostgreSQLReceiver
from etl_core.components.envelopes import Out


@register_component("read_postgresql")
class PostgreSQLRead(PostgreSQLComponent):
    """PostgreSQL reader supporting row, bulk, and bigdata modes."""

    ALLOW_NO_INPUTS = True  # This is a source component that doesn't need input ports
    
    params: Dict[str, Any] = Field(default_factory=dict, description="Query parameters")

    @model_validator(mode="after")
    def _build_objects(self):
        """Build objects after validation."""
        self._receiver = PostgreSQLReceiver()
        return self

    async def process_row(
        self, payload: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Read rows one-by-one (streaming)."""
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
            query=self.query,
            params=self.params,
            connection_handler=self.connection_handler,
        )
        yield Out(port="out", payload=frame)

    async def process_bigdata(
        self,
        payload: Any,
        metrics: ComponentMetrics,
    ) -> AsyncIterator[Out]:
        """Read large query results as a Dask DataFrame."""
        ddf: dd.DataFrame = await self._receiver.read_bigdata(
            entity_name=self.entity_name,
            metrics=metrics,
            query=self.query,
            params=self.params,
            connection_handler=self.connection_handler,
        )
        yield Out(port="out", payload=ddf)
