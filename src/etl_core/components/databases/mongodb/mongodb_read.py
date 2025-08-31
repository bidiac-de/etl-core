from __future__ import annotations

from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

from pydantic import Field, PrivateAttr, model_validator

from etl_core.components.component_registry import register_component
from etl_core.components.databases.mongodb.mongodb import MongoDBComponent
from etl_core.components.envelopes import Out
from etl_core.components.wiring.ports import OutPortSpec
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.databases.mongodb.mongodb_receiver import MongoDBReceiver
from etl_core.utils.common_helpers import leaf_field_paths


@register_component("read_mongodb")
class MongoDBRead(MongoDBComponent):
    """
    MongoDB reader supporting row, bulk, and bigdata modes.
    Produces on a single 'out' port.

    The MongoDB projection is ALWAYS derived from the 'out' port Schema:
      - Include all leaf paths as {path: 1} (dot-separated).
      - Exclude '_id' unless present in the schema (add {'_id': 0}).
    No explicit 'projection' can be set on this component.
    """

    OUTPUT_PORTS = (OutPortSpec(name="out", required=True, fanout="many"),)
    ALLOW_NO_INPUTS = True

    # Query controls (user-configurable)
    query_filter: Dict[str, Any] = Field(
        default_factory=dict, description="find() filter"
    )
    sort: Optional[List[Tuple[str, int]]] = Field(
        default=None,
        description="List of (field, direction) where direction in {1, -1}",
    )
    limit: Optional[int] = Field(default=None, description="Max documents to read")
    skip: int = Field(default=0, ge=0, description="Documents to skip")

    # Private state
    _receiver: MongoDBReceiver = PrivateAttr(default_factory=MongoDBReceiver)
    _projection: Optional[Dict[str, int]] = PrivateAttr(default=None)

    @model_validator(mode="after")
    def _build_objects(self) -> "MongoDBRead":
        self._receiver = MongoDBReceiver()
        # Connection and receiver are set up at build time.
        self._setup_connection()
        # Build projection from the out port schema once.
        self._projection = self._projection_from_out_schema()
        return self

    def _projection_from_out_schema(self) -> Optional[Dict[str, int]]:
        """
        Build a MongoDB projection from the 'out' port Schema.

        Behavior:
        - Include all flattened leaf paths as {path: 1}.
        - If '_id' is not part of the schema, add {'_id': 0} to exclude it.
        - If there is no 'out' schema available, return None (no projection).
        """

        schema = self.out_port_schemas["out"]

        # Collect dot-separated leaf paths (nested fields become 'a.b.c').
        paths = leaf_field_paths(schema, ".")
        if not paths:
            return None

        proj: Dict[str, int] = {p: 1 for p in paths if p != "_id"}
        if "_id" not in paths:
            proj["_id"] = 0
        return proj

    async def process_row(self, metrics: ComponentMetrics) -> AsyncIterator[Out]:
        # Yield each document as it arrives to keep true streaming behavior
        async for doc in self._receiver.read_row(
            connection_handler=self.connection_handler,
            database_name=self._database_name,
            entity_name=self.entity_name,
            metrics=metrics,
            query_filter=self.query_filter,
            projection=self._projection,
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
            projection=self._projection,
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
            projection=self._projection,
            sort=self.sort,
            limit=self.limit,
            skip=self.skip,
            chunk_size=self.bigdata_partition_chunk_size,
        ):
            yield Out(port="out", payload=ddf)
