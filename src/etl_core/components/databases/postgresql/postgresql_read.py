from typing import Any, Dict, AsyncIterator
import pandas as pd
import dask.dataframe as dd
from pydantic import Field, model_validator

from etl_core.components.databases.postgresql.postgresql import PostgreSQLComponent
from etl_core.components.component_registry import register_component
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.databases.postgresql.postgresql_receiver import PostgreSQLReceiver


@register_component("read_postgresql")
class PostgreSQLRead(PostgreSQLComponent):
    """PostgreSQL reader supporting row, bulk, and bigdata modes."""

    query: str = Field(..., description="SQL query to execute")
    params: Dict[str, Any] = Field(default_factory=dict, description="Query parameters")

    @model_validator(mode="after")
    def _build_objects(self):
        """Build objects after validation."""
        self._receiver = PostgreSQLReceiver()
        return self

    async def process_row(
        self, payload: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Dict[str, Any]]:
        """Read rows one-by-one (streaming)."""
        async for result in self._receiver.read_row(
            entity_name=self.entity_name,
            metrics=metrics,
            query=self.query,
            params=self.params,
            connection_handler=self.connection_handler,
        ):
            yield result

    async def process_bulk(
        self,
        payload: Any,
        metrics: ComponentMetrics,
    ) -> pd.DataFrame:
        """Read query results as a pandas DataFrame."""
        return await self._receiver.read_bulk(
            entity_name=self.entity_name,
            metrics=metrics,
            query=self.query,
            params=self.params,
            connection_handler=self.connection_handler,
        )

    async def process_bigdata(
        self,
        payload: Any,
        metrics: ComponentMetrics,
    ) -> dd.DataFrame:
        """Read large query results as a Dask DataFrame."""
        return await self._receiver.read_bigdata(
            entity_name=self.entity_name,
            metrics=metrics,
            query=self.query,
            params=self.params,
            connection_handler=self.connection_handler,
        )
