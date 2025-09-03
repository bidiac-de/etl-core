from __future__ import annotations

from enum import Enum
from typing import Any, AsyncIterator, Dict, List, Tuple, Union, Set

import dask.dataframe as dd
import pandas as pd
from pydantic import BaseModel, Field, PrivateAttr, model_validator

from etl_core.components.component_registry import register_component
from etl_core.components.data_operations.data_operations import (  # noqa: E501
    DataOperationsComponent,
)
from etl_core.components.envelopes import Out
from etl_core.components.wiring.ports import InPortSpec, OutPortSpec
from etl_core.components.wiring.schema import Schema
from etl_core.job_execution.job_execution_handler import InTagged
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa: E501
    DataOperationsMetrics,
)
from etl_core.receivers.data_operations_receivers.aggregation.aggregation_receiver import (  # noqa: E501
    AggregationReceiver,
)


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
        # Keep Enum value intact; receiver normalizes enums/strings
        return {"src": self.src, "op": self.op, "dest": self.dest}


@register_component("aggregation")
class AggregationComponent(DataOperationsComponent):
    """
    Group-by + aggregation with partition buffering for row/bulk/bigdata.

    Input policy (mirrors SchemaMapping):
      - Accept either plain payloads or InTagged envelopes.
      - Ellipsis signals flush/closure for the given in-port.
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
        self._reset_buffers()
        self._prevalidate_against_schema()
        return self

    def _reset_buffers(self) -> None:
        self._row_buf = {}
        self._bulk_buf = {}
        self._big_buf = {}

    def _input_field_names(self) -> Set[str]:
        """
        Extract top-level field names from declared input schema, if present.
        Works with either Schema objects or dict-style schemas (tests).
        """
        schema_like = self.in_port_schemas.get("in")
        if isinstance(schema_like, Schema):
            return {f.name for f in schema_like.fields}
        if isinstance(schema_like, dict):
            fields = schema_like.get("fields") or []
            return {str(f.get("name")) for f in fields if isinstance(f, dict)}
        return set()

    def _prevalidate_against_schema(self) -> None:
        """
        Fail fast if group_by/src fields are not present in the declared input schema.
        Skips '*' since it denotes row-count.
        """
        names = self._input_field_names()
        if not names:
            # No declared schema -> skip strict validation (keep runtime errors)
            return

        missing_gb = [c for c in self.group_by if c not in names]
        if missing_gb:
            raise ValueError(
                f"{self.name}: group_by contains fields not in input schema: {missing_gb}"
            )

        missing_src = [
            a.src for a in self.aggregations if a.src != "*" and a.src not in names
        ]
        if missing_src:
            raise ValueError(
                f"{self.name}: aggregations reference missing fields: {sorted(set(missing_src))}"
            )


    def requires_tagged_input(self) -> bool:
        # component is buffering, needs to know end-of-stream
        # which comes via InTagged envelope
        return True

    @staticmethod
    def _unwrap(obj: Any, default_port: str) -> Tuple[str, Any]:
        """
        Accept both InTagged and raw payloads from the runtime.
        Returns (in_port, payload).
        """
        if isinstance(obj, InTagged):
            return obj.in_port, obj.payload
        return default_port, obj


    async def process_row(
        self,
        row: Union[Dict[str, Any], InTagged],
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        in_port, payload = self._unwrap(row, "in")

        if payload is Ellipsis:
            rows = self._row_buf.pop(in_port, [])
            async for out_port, out_payload in self._receiver.process_rows(
                rows=rows,
                group_by=self.group_by,
                aggregations=[op.to_dict() for op in self.aggregations],
                metrics=metrics,
            ):
                yield Out(port=out_port, payload=out_payload)
            return

        # buffer until flush
        if isinstance(payload, dict):
            self._row_buf.setdefault(in_port, []).append(payload)


    async def process_bulk(
        self,
        dataframe: Union[pd.DataFrame, InTagged],
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        in_port, payload = self._unwrap(dataframe, "in")

        if payload is Ellipsis:
            pdf = self._bulk_buf.pop(in_port, None)
            if pdf is None:
                # empty output with no columns
                yield Out(port="out", payload=pd.DataFrame().head(0))
                return
            async for out_port, out_payload in self._receiver.process_bulk(
                dataframe=pdf,
                group_by=self.group_by,
                aggregations=[op.to_dict() for op in self.aggregations],
                metrics=metrics,
            ):
                yield Out(port=out_port, payload=out_payload)
            return

        if isinstance(payload, pd.DataFrame):
            cur = self._bulk_buf.get(in_port)
            if cur is None:
                self._bulk_buf[in_port] = payload
            else:
                self._bulk_buf[in_port] = pd.concat([cur, payload], ignore_index=True)


    async def process_bigdata(
        self,
        ddf: Union[dd.DataFrame, InTagged],
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        in_port, payload = self._unwrap(ddf, "in")

        if payload is Ellipsis:
            buf = self._big_buf.pop(in_port, None)
            if buf is None:
                empty = dd.from_pandas(pd.DataFrame().head(0), npartitions=1)
                yield Out(port="out", payload=empty)
                return
            async for out_port, out_payload in self._receiver.process_bigdata(
                ddf=buf,
                group_by=self.group_by,
                aggregations=[op.to_dict() for op in self.aggregations],
                metrics=metrics,
            ):
                yield Out(port=out_port, payload=out_payload)
            return

        if isinstance(payload, dd.DataFrame):
            cur = self._big_buf.get(in_port)
            if cur is None:
                self._big_buf[in_port] = payload
            else:
                self._big_buf[in_port] = dd.concat([cur, payload])
