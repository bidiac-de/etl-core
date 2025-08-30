from typing import Any, Dict, AsyncIterator
import pandas as pd
import dask.dataframe as dd
from pydantic import model_validator

from etl_core.components.databases.postgresql.postgresql import PostgreSQLComponent
from etl_core.components.component_registry import register_component
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.databases.postgresql.postgresql_receiver import (
    PostgreSQLReceiver,
)
from etl_core.components.envelopes import Out
from etl_core.components.wiring.ports import InPortSpec, OutPortSpec


@register_component("write_postgresql")
class PostgreSQLWrite(PostgreSQLComponent):
    """
    PostgreSQL writer with ports + schema.

    - INPUT_PORTS:
        - 'in' (required): rows/frames to write
    - OUTPUT_PORTS:
        - 'out' (optional): passthrough of what was written (useful for chaining/tests)
    """

    INPUT_PORTS = (InPortSpec(name="in", required=True, fanin="many"),)
    OUTPUT_PORTS = (OutPortSpec(name="out", required=False, fanout="many"),)

    @model_validator(mode="after")
    def _build_objects(self):
        """Build objects after validation."""
        # Create and assign the PostgreSQL receiver
        self._receiver = PostgreSQLReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Write a single row and emit it (or receiver result) on 'out'."""
        # Build query based on if_exists strategy
        columns = list(row.keys())
        query = self._build_upsert_query(self.entity_name, columns, self.if_exists)

        result = await self._receiver.write_row(
            entity_name=self.entity_name,
            row=row,
            metrics=metrics,
            query=query,
            connection_handler=self.connection_handler,
        )
        yield Out(port="out", payload=result)

    async def process_bulk(
        self, data: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Write a pandas DataFrame and emit the same frame on 'out'."""
        # Build query based on if_exists strategy
        columns = list(data.columns)
        query = self._build_upsert_query(self.entity_name, columns, self.if_exists)

        result = await self._receiver.write_bulk(
            entity_name=self.entity_name,
            frame=data,
            metrics=metrics,
            query=query,
            connection_handler=self.connection_handler,
        )
        yield Out(port="out", payload=result)

    async def process_bigdata(
        self, ddf: dd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Write a Dask DataFrame and emit the same ddf on 'out'."""
        # Build query based on if_exists strategy
        # Get columns from first partition
        first_partition = ddf.map_partitions(lambda pdf: pdf).partitions[0].compute()
        columns = list(first_partition.columns)
        query = self._build_upsert_query(self.entity_name, columns, self.if_exists)

        result = await self._receiver.write_bigdata(
            entity_name=self.entity_name,
            frame=ddf,
            metrics=metrics,
            query=query,
            connection_handler=self.connection_handler,
        )
        yield Out(port="out", payload=result)
