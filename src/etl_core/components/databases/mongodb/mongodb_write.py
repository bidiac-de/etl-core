from __future__ import annotations

from typing import Any, AsyncIterator, Dict, List, Optional, Set

import dask.dataframe as dd
import pandas as pd
from pydantic import Field, model_validator, PrivateAttr

from etl_core.components.component_registry import register_component
from etl_core.components.databases.database_operation_mixin import (
    DatabaseOperationMixin,
)
from etl_core.components.databases.if_exists_strategy import DatabaseOperation
from etl_core.components.databases.mongodb.mongodb import MongoDBComponent
from etl_core.components.envelopes import Out
from etl_core.components.wiring.ports import InPortSpec, OutPortSpec
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.receivers.databases.mongodb.mongodb_receiver import MongoDBReceiver


@register_component("write_mongodb")
class MongoDBWrite(MongoDBComponent, DatabaseOperationMixin):
    """
    MongoDB writer for row/bulk/bigdata.
    - INPUT: 'in' (required)
    - OUTPUT: 'out' (optional passthrough)
    """

    INPUT_PORTS = (InPortSpec(name="in", required=True, fanin="many"),)
    OUTPUT_PORTS = (OutPortSpec(name="out", required=False, fanout="many"),)

    key_fields: List[str] = Field(
        default_factory=list,
        description="Fields to derive a match filter for upsert/update",
    )
    update_fields: Optional[Set[str]] = Field(
        default=None,
        description="If set, only update these fields ($set).",
    )
    match_filter: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Explicit filter for UPDATE; overrides key_fields if given.",
    )
    ordered: bool = Field(default=True, description="Ordered bulk writes")

    _write_options: Dict[str, Any] = PrivateAttr(default_factory=dict)

    @model_validator(mode="after")
    def _build_objects(self) -> "MongoDBComponent":
        self._receiver = MongoDBReceiver()
        self._setup_connection()
        return self

    @model_validator(mode="after")
    def _post_init(self) -> "MongoDBWrite":
        if self.operation in {DatabaseOperation.UPSERT, DatabaseOperation.UPDATE}:
            if not self.key_fields and not self.match_filter:
                raise ValueError(
                    f"{self.name}: key_fields or match_filter required in update mode"
                )
        self._write_options = {
            "key_fields": list(self.key_fields),
            "update_fields": sorted(self.update_fields) if self.update_fields else None,
            "match_filter": self.match_filter,
            "ordered": self.ordered,
        }
        return self

    async def process_row(
        self, row: Dict[str, Any], metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        result = await self._receiver.write_row(
            connection_handler=self.connection_handler,
            database_name=self.database_name or "",
            entity_name=self.entity_name,
            row=row,
            metrics=metrics,
            operation=self.operation,
            write_options=self._write_options,
        )
        yield Out(port="out", payload=result)

    async def process_bulk(
        self, frame: pd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        async for result in self._receiver.write_bulk(
            connection_handler=self.connection_handler,
            database_name=self.database_name or "",
            entity_name=self.entity_name,
            frame=frame,
            metrics=metrics,
            operation=self.operation,
            write_options=self._write_options,
        ):
            yield Out(port="out", payload=result)

    async def process_bigdata(
        self, ddf: dd.DataFrame, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        async for result in self._receiver.write_bigdata(
            connection_handler=self.connection_handler,
            database_name=self.database_name or "",
            entity_name=self.entity_name,
            frame=ddf,
            metrics=metrics,
            operation=self.operation,
            write_options=self._write_options,
        ):
            yield Out(port="out", payload=result)
