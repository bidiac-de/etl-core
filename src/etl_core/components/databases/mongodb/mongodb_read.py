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
    MongoDB reader supporting row, bulk, and bigdata strategies.

    Projection is derived from the 'out' schema:
      - include all leaf paths as {path: 1}
      - exclude '_id' unless '_id' is present in the schema
      - if no 'out' schema is available, projection is None (read all)
    """

    OUTPUT_PORTS = (OutPortSpec(name="out", required=True, fanout="many"),)
    ALLOW_NO_INPUTS = True

    # Query controls (user-configurable)
    query_filter: Dict[str, Any] = Field(
        default_factory=dict, description="find() filter"
    )
    sort: Optional[List[Tuple[str, int]]] = Field(
        default=None,
        description="List of (field, direction) pairs where direction in {1, -1}",
    )
    limit: Optional[int] = Field(default=None, description="Max documents to read")
    skip: int = Field(default=0, ge=0, description="Documents to skip from start")

    # Private state
    _receiver: MongoDBReceiver = PrivateAttr(default_factory=MongoDBReceiver)
    _projection: Optional[Dict[str, int]] = PrivateAttr(default=None)

    @model_validator(mode="after")
    def _build_objects(self) -> "MongoDBRead":
        self._receiver = MongoDBReceiver()
        self._setup_connection()
        # Build projection once from the out port schema
        self._projection = self._projection_from_out_schema()
        return self

    def _projection_from_out_schema(self) -> Optional[Dict[str, int]]:
        """
        Build a MongoDB projection from the 'out' port Schema.

        Behavior:
        - include all flattened leaf paths as {path: 1}
        - if '_id' is not part of the schema, add {'_id': 0} to exclude it
        - if there is no 'out' schema, return None (no projection)
        """
        schema = self.out_port_schemas.get("out")
        if not schema:
            return None

        paths = leaf_field_paths(schema, self._schema_path_separator)
        if not paths:
            return None

        proj: Dict[str, int] = {p: 1 for p in paths if p != "_id"}
        if "_id" not in paths:
            proj["_id"] = 0
        return proj

    async def process_row(
        self, _payload: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """
        RowStrategy calls: process_row(payload, metrics).
        Readers ignore the incoming payload (should be None).
        """
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

    async def process_bulk(
        self, _payload: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """
        BulkStrategy calls: process_bulk(payload, metrics).
        Readers ignore the incoming payload (should be None)
        and emit pandas.DataFrame chunks.
        """
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
            seperator=self._schema_path_separator,
        ):
            yield Out(port="out", payload=frame)

    async def process_bigdata(
        self, _payload: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        """
        BigDataStrategy calls: process_bigdata(payload, metrics).
        Readers ignore the incoming payload (should be None) and emit a
         Dask DataFrame per partition.
        """
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
            seperator=self._schema_path_separator,
        ):
            yield Out(port="out", payload=ddf)
