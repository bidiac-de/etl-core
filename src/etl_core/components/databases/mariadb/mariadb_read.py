from typing import Any, Dict, AsyncIterator
import pandas as pd
import dask.dataframe as dd
from pydantic import Field, model_validator

from src.etl_core.components.databases.mariadb.mariadb import MariaDBComponent
from src.etl_core.components.component_registry import register_component
from src.etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.databases.mariadb.mariadb_receiver import MariaDBReceiver


@register_component("read_mariadb")
class MariaDBRead(MariaDBComponent):
    """MariaDB reader supporting row, bulk, and bigdata modes."""

    query: str = Field(..., description="SQL query to execute")
    params: Dict[str, Any] = Field(default_factory=dict, description="Query parameters")
    strategy_type: str = Field(default="bulk", description="Execution strategy type")

    @model_validator(mode="after")
    def _build_objects(self):
        """Build objects after validation."""
        self._receiver = MariaDBReceiver(self.connection_handler)
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
        )
