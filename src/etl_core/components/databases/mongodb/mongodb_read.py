from __future__ import annotations

from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

from pydantic import Field

from etl_core.components.component_registry import register_component
from etl_core.components.databases.mongodb.mongodb import MongoDBComponent
from etl_core.components.envelopes import Out
from etl_core.components.wiring.ports import OutPortSpec
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics


@register_component("read_mongodb")
class MongoDBRead(MongoDBComponent):
    """
    MongoDB reader supporting row, bulk, and bigdata modes.
    Produces on a single 'out' port.
    """

    OUTPUT_PORTS = (OutPortSpec(name="out", required=True, fanout="many"),)
    ALLOW_NO_INPUTS = True

    query_filter: Dict[str, Any] = Field(default_factory=dict, description="find() filter")
    projection: Optional[Dict[str, int]] = Field(default=None, description="find() projection")
    sort: Optional[List[Tuple[str, int]]] = Field(
        default=None, description="List of (field, direction) where direction in {1, -1}"
    )
    limit: Optional[int] = Field(default=None, description="Max documents to read")
    skip: int = Field(default=0, ge=0, description="Documents to skip")

    async def process_row(self, metrics: ComponentMetrics) -> AsyncIterator[Out]:
        async for doc in self._receiver.read_row(
            connection_handler=self.connection_handler,
            database_name=self._database_name,
            entity_name=self.entity_name,
            metrics=metrics,
            query_filter=self.query_filter,
            projection=self.projection,
            sort=self.sort,
            limit=self.limit,
            skip=self.skip,
            batch_size=self.row_batch_size,
        ):
            yield Out(port="out", payload=doc)

    async def process_bulk(self, metrics: ComponentMetrics) -> AsyncIterator[Out]:
        async for frame in self._receiver.read_bulk(
            connection_handler=self.connection_handler,
            database_name=self._database_name,
            entity_name=self.entity_name,
            metrics=metrics,
            query_filter=self.query_filter,
            projection=self.projection,
            sort=self.sort,
            limit=self.limit,
            skip=self.skip,
            chunk_size=self.bulk_chunk_size,
        ):
            yield Out(port="out", payload=frame)

    async def process_bigdata(self, metrics: ComponentMetrics) -> AsyncIterator[Out]:
        async for ddf in self._receiver.read_bigdata(
            connection_handler=self.connection_handler,
            database_name=self._database_name,
            entity_name=self.entity_name,
            metrics=metrics,
            query_filter=self.query_filter,
            projection=self.projection,
            sort=self.sort,
            limit=self.limit,
            skip=self.skip,
            chunk_size=self.bigdata_partition_chunk_size,
        ):
            yield Out(port="out", payload=ddf)
