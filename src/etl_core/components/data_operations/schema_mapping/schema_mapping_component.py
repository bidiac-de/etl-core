from __future__ import annotations

from typing import Any, AsyncIterator, Dict, Iterable, List, Tuple

import dask.dataframe as dd
import pandas as pd
from pydantic import Field, model_validator

from etl_core.components.data_operations.data_operations import DataOperationsComponent
from etl_core.components.data_operations.schema_mapping.mapping_rule import FieldMapping
from etl_core.components.envelopes import Out
from etl_core.components.component_registry import register_component
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa: E501
    DataOperationsMetrics,
)
from etl_core.receivers.data_operations_receivers.schema_mapping.schema_mapping_receiver import (  # noqa: E501
    SchemaMappingReceiver,
)


@register_component("schema_mapping")
class SchemaMappingComponent(DataOperationsComponent):
    """
    Orchestrates schema mapping by delegating to SchemaMappingReceiver.
    Strategies call the process methods. We forward what the receiver yields
    and assemble it into Out(port=..., payload=...).
    """

    rules: List[FieldMapping] = Field(
        default_factory=list,
        description="List of field mapping rules.",
    )

    @model_validator(mode="after")
    def _build_objects(self) -> "SchemaMappingComponent":
        self._receiver = SchemaMappingReceiver()
        return self

    def _tuple_rules(self) -> Iterable[Tuple[str, str, str, str]]:
        for r in self.rules:
            yield r.src_port, r.src_path, r.dst_port, r.dst_path

    async def process_row(
        self,
        row: Dict[str, Any],
        *,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        """
        Map a single row and emit one Out per destination port produced.
        """
        async for port, payload in self._receiver.process_row(
            row,
            metrics=metrics,
            rules=self._tuple_rules(),
        ):
            yield Out(port=port, payload=payload)

    async def process_bulk(
        self,
        dataframe: pd.DataFrame,
        *,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        """
        Map a pandas DataFrame and emit one Out per destination port produced.
        """
        async for port, payload in self._receiver.process_bulk(
            dataframe,
            metrics=metrics,
            rules=self._tuple_rules(),
        ):
            yield Out(port=port, payload=payload)

    async def process_bigdata(
        self,
        ddf: dd.DataFrame,
        *,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        """
        Map a Dask DataFrame and emit one Out per destination port produced.
        """
        async for port, payload in self._receiver.process_bigdata(
            ddf,
            metrics=metrics,
            rules=self._tuple_rules(),
        ):
            yield Out(port=port, payload=payload)
