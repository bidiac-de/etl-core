from __future__ import annotations

from typing import Any, AsyncIterator, Dict

import dask.dataframe as dd
import pandas as pd
from pydantic import Field, model_validator

from etl_core.components.component_registry import register_component
from etl_core.components.databases.mariadb.mariadb import MariaDBComponent
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.components.envelopes import Out
from etl_core.components.wiring.ports import InPortSpec, OutPortSpec
from etl_core.receivers.databases.mariadb.mariadb_receiver import MariaDBReceiver


@register_component("write_mariadb")
class MariaDBWrite(MariaDBComponent):
    """
    MariaDB writer with ports + schema.

    - INPUT_PORTS:
        - 'in' (required): rows/frames to write
    - OUTPUT_PORTS:
        - 'out' (optional): passthrough of what was written (useful for chaining/tests)
    """

    INPUT_PORTS = (InPortSpec(name="in", required=True, fanin="many"),)
    OUTPUT_PORTS = (OutPortSpec(name="out", required=False, fanout="many"),)

    if_exists: str = Field(default="append", description="How to behave if the table already exists")

    @model_validator(mode="after")
    def _build_objects(self) -> "MariaDBWrite":
        self._receiver = MariaDBReceiver()
        return self

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """Write a single row and emit it (or receiver result) on 'out'."""
        result = await self._receiver.write_row(
            entity_name=self.entity_name,
            row=row,
            metrics=metrics,
            table=self.entity_name,
            query=self.query,
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
            table=self.entity_name,
            query=self.query,
            if_exists=self.if_exists,
            bulk_chunk_size=self.bulk_chunk_size,
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
            table=self.entity_name,
            query=self.query,
            if_exists=self.if_exists,
            bigdata_partition_chunk_size=self.bigdata_partition_chunk_size,
            connection_handler=self.connection_handler,
        )
        yield Out(port="out", payload=result)
