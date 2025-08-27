from typing import Any, Dict, AsyncIterator
import pandas as pd
import dask.dataframe as dd
from pydantic import Field, model_validator

from src.etl_core.components.databases.mariadb.mariadb import MariaDBComponent
from src.etl_core.components.component_registry import register_component
from src.etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.databases.mariadb.mariadb_receiver import MariaDBReceiver


@register_component("write_mariadb")
class MariaDBWrite(MariaDBComponent):
    """MariaDB writer supporting row, bulk, and bigdata modes."""

    batch_size: int = Field(default=1000, description="Batch size for bulk operations")

    @model_validator(mode="after")
    def _build_objects(self):
        """Build objects after validation."""
        # Create and assign the MariaDB receiver
        self._receiver = MariaDBReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncIterator[Dict[str, Any]]:
        """Write a single row and yield the result."""
        result = await self._receiver.write_row(
            entity_name=self.entity_name,
            row=row,
            metrics=metrics,
            table=self.entity_name,
            query=self.query,
            connection_handler=self.connection_handler,
        )
        yield result

    async def process_bulk(
        self, data: pd.DataFrame, metrics: ComponentMetrics
    ) -> pd.DataFrame:
        """Write full dataset and yield it as DataFrame."""
        result = await self._receiver.write_bulk(
            entity_name=self.entity_name,
            frame=data,
            metrics=metrics,
            table=self.entity_name,
            query=self.query,
            connection_handler=self.connection_handler,
        )
        return result

    async def process_bigdata(
        self, chunk_iterable: dd.DataFrame, metrics: ComponentMetrics
    ) -> dd.DataFrame:
        """Write Dask DataFrame and yield it."""
        result = await self._receiver.write_bigdata(
            entity_name=self.entity_name,
            frame=chunk_iterable,
            metrics=metrics,
            table=self.entity_name,
            query=self.query,
            connection_handler=self.connection_handler,
        )
        return result
