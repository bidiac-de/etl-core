from __future__ import annotations

from enum import Enum
from typing import Any, AsyncIterator, Dict, List

import dask.dataframe as dd
import pandas as pd
from pydantic import BaseModel, Field, PrivateAttr, model_validator

from etl_core.components.base_component import Component
from etl_core.components.component_registry import register_component
from etl_core.components.envelopes import Out
from etl_core.components.wiring.ports import InPortSpec, OutPortSpec
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa: E501
    DataOperationsMetrics,
)
from etl_core.receivers.data_operations_receivers.aggregation.aggregation_receiver import (  # noqa: E501
    AggregationReceiver,
)
from etl_core.job_execution.job_execution_handler import InTagged


class AggregationOp(str, Enum):
    """Supported aggregation operations."""

    COUNT = "count"
    SUM = "sum"
    MIN = "min"
    MAX = "max"
    MEAN = "mean"
    MEDIAN = "median"
    STD = "std"
    NUNIQUE = "nunique"


class AggOp(BaseModel):
    """One aggregation instruction."""

    src: str = Field(..., description="Source field (or '*' for row count)")
    op: AggregationOp = Field(..., description="Aggregation operation")
    dest: str = Field(..., description="Destination field name")

    def to_dict(self) -> Dict[str, Any]:
        return {"src": self.src, "op": self.op, "dest": self.dest}


@register_component("aggregation")
class AggregationComponent(Component):
    """
    Group-by + aggregation with partition buffering for row/bulk/bigdata.
    """

    INPUT_PORTS = (InPortSpec(name="in", required=True, fanin="many"),)
    OUTPUT_PORTS = (OutPortSpec(name="out", required=True, fanout="many"),)

    group_by: List[str] = Field(default_factory=list, description="Dotted group keys.")
    aggregations: List[AggOp] = Field(
        default_factory=list, description="Aggregation operations."
    )

    _receiver: AggregationReceiver = PrivateAttr()
    _row_buf: Dict[str, List[Dict[str, Any]]] = PrivateAttr(default_factory=dict)
    _bulk_buf: Dict[str, pd.DataFrame] = PrivateAttr(default_factory=dict)
    _big_buf: Dict[str, dd.DataFrame] = PrivateAttr(default_factory=dict)

    @model_validator(mode="after")
    def _build_objects(self) -> "AggregationComponent":
        self._receiver = AggregationReceiver()
        self._row_buf = {}
        self._bulk_buf = {}
        self._big_buf = {}
        return self

    def requires_tagged_input(self) -> bool:
        return True

    async def process_row(
        self,
        row: InTagged,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        if row.payload is Ellipsis:
            rows = self._row_buf.pop(row.in_port, [])
            async for out_port, payload in self._receiver.process_rows(
                rows=rows,
                group_by=self.group_by,
                aggregations=[op.to_dict() for op in self.aggregations],
                metrics=metrics,
            ):
                yield Out(port=out_port, payload=payload)
            return

        self._row_buf.setdefault(row.in_port, []).append(row.payload)

    async def process_bulk(
        self,
        dataframe: InTagged,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        if dataframe.payload is Ellipsis:
            pdf = self._bulk_buf.pop(dataframe.in_port, None)
            if pdf is None:
                yield Out(port="out", payload=pd.DataFrame().head(0))
                return

            async for out_port, payload in self._receiver.process_bulk(
                dataframe=pdf,
                group_by=self.group_by,
                aggregations=[op.to_dict() for op in self.aggregations],
                metrics=metrics,
            ):
                yield Out(port=out_port, payload=payload)
            return

        cur = self._bulk_buf.get(dataframe.in_port)
        if cur is None:
            self._bulk_buf[dataframe.in_port] = dataframe.payload
        else:
            self._bulk_buf[dataframe.in_port] = pd.concat(
                [cur, dataframe.payload],
                ignore_index=True,
            )

    async def process_bigdata(
        self,
        ddf: InTagged,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        if ddf.payload is Ellipsis:
            buf = self._big_buf.pop(ddf.in_port, None)
            if buf is None:
                empty = dd.from_pandas(pd.DataFrame().head(0), npartitions=1)
                yield Out(port="out", payload=empty)
                return

            async for out_port, payload in self._receiver.process_bigdata(
                ddf=buf,
                group_by=self.group_by,
                aggregations=[op.to_dict() for op in self.aggregations],
                metrics=metrics,
            ):
                yield Out(port=out_port, payload=payload)
            return

        cur = self._big_buf.get(ddf.in_port)
        if cur is None:
            self._big_buf[ddf.in_port] = ddf.payload
        else:
            self._big_buf[ddf.in_port] = dd.concat([cur, ddf.payload])
