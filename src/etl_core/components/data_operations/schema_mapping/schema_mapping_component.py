from __future__ import annotations

from typing import Any, AsyncIterator, Dict, List, Tuple, Union

import dask.dataframe as dd
import pandas as pd
from pydantic import Field, PrivateAttr, model_validator

from etl_core.components.data_operations.data_operations import (
    DataOperationsComponent,
)
from etl_core.components.data_operations.schema_mapping.mapping_rule import (
    FieldMapping,
    validate_field_mappings,
)
from etl_core.components.data_operations.schema_mapping.join_rules import (
    JoinPlan,
    validate_join_plan,
)
from etl_core.receivers.data_operations_receivers.schema_mapping.schema_mapping_receiver import (  # noqa: E501
    SchemaMappingReceiver,
)
from etl_core.components.wiring.schema import Schema
from etl_core.job_execution.job_execution_handler import InTagged, Out
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa: E501
    DataOperationsMetrics,
)


StrRule = Tuple[str, str, str, str]


class SchemaMappingComponent(DataOperationsComponent):
    """
    Orchestrates mapping and joins

    - Owns buffering for row/bulk/bigdata
    - Receiver is stateless; we pass everything it needs per call
    - Metrics are updated exclusively inside the receiver
    """

    rules: List[FieldMapping] = Field(default_factory=list)
    join_plan: JoinPlan = Field(default_factory=JoinPlan)

    _receiver: SchemaMappingReceiver = PrivateAttr()

    _row_buf: Dict[str, List[Dict[str, Any]]] = PrivateAttr(default_factory=dict)
    _bulk_buf: Dict[str, pd.DataFrame] = PrivateAttr(default_factory=dict)
    _big_buf: Dict[str, dd.DataFrame] = PrivateAttr(default_factory=dict)
    _closed_ports: set[str] = PrivateAttr(default_factory=set)
    _required_ports: set[str] = PrivateAttr(default_factory=set)

    @model_validator(mode="after")
    def _build_objects(self) -> "SchemaMappingComponent":
        # Set up a fresh receiver instance for pure operations
        self._receiver = SchemaMappingReceiver()

        # Validate mapping rules against port schemas
        validate_field_mappings(
            self.rules,
            in_port_schemas=self.in_port_schemas,
            out_port_schemas=self.out_port_schemas,
            component_name=self.name,
            path_separator=self._schema_path_separator,
        )
        # Validate join plan (ports exist, keys exist)
        validate_join_plan(
            self.join_plan,
            in_port_schemas=self.in_port_schemas,
            out_port_names={p.name for p in self.expected_ports()},
            component_name=self.name,
            path_separator=self._schema_path_separator,
        )

        # Precompute required input ports for join completion checks
        self._required_ports = self._compute_required_ports(
            self.join_plan, self.in_port_schemas
        )
        self._reset_buffers()
        return self

    def requires_tagged_input(self) -> bool:
        # Needed when join is defined and more than one input port is active
        return bool(self.join_plan.steps) and len(self.expected_in_port_names()) > 1

    async def process_row(
        self,
        row: Union[Dict[str, Any], InTagged],
        *,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        # Join-mode row processing uses tagged envelopes
        if self._is_tagged_join(row):
            async for out in self._process_row_tagged_join(row, metrics):
                yield out
            return

        # Mapping-only: transform a single row by rules
        if isinstance(row, dict):
            async for out in self._process_row_mapping(row, metrics):
                yield out

    async def _process_row_tagged_join(
        self,
        row: InTagged,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        # Buffer rows per-port; Ellipsis signals that a port is closed
        if row.payload is Ellipsis:
            self._closed_ports.add(row.in_port)
        else:
            self._row_buf.setdefault(row.in_port, []).append(row.payload)

        # Join once all required ports have been closed
        if not self._ready_to_join():
            return

        results = self._receiver.run_row_joins(
            buffers=self._row_buf,
            join_plan=self.join_plan,
            metrics=metrics,
        )
        for port, rows in results.items():
            for payload in rows:
                yield Out(port=port, payload=payload)
        self._reset_buffers()

    async def _process_row_mapping(
        self,
        row: Dict[str, Any],
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        srules = self._rules_as_tuples()
        for port, payload in self._receiver.map_row(
            row=row,
            rules=srules,
            metrics=metrics,
        ):
            yield Out(port=port, payload=payload)

    async def process_bulk(
        self,
        dataframe: Union[pd.DataFrame, InTagged],
        *,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        # Tagged envelopes for bulk joins; plain frames for mapping
        if self._is_tagged_join(dataframe):
            async for out in self._process_bulk_tagged_join(dataframe, metrics):
                yield out
            return

        if isinstance(dataframe, pd.DataFrame):
            async for out in self._process_bulk_mapping(dataframe, metrics):
                yield out

    async def _process_bulk_tagged_join(
        self,
        dataframe: InTagged,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        # Buffer frames per port; concatenate chunks until closed
        if dataframe.payload is Ellipsis:
            self._closed_ports.add(dataframe.in_port)
        else:
            df = dataframe.payload
            cur = self._bulk_buf.get(dataframe.in_port)
            if cur is None:
                self._bulk_buf[dataframe.in_port] = df
            else:
                self._bulk_buf[dataframe.in_port] = pd.concat(
                    [cur, df], ignore_index=True
                )

        if not self._ready_to_join():
            return

        results = self._receiver.run_bulk_joins(
            buffers=self._bulk_buf,
            join_plan=self.join_plan,
            out_port_schemas=self.out_port_schemas,
            path_separator=self._schema_path_separator,
            metrics=metrics,
        )
        for port, out_df in results.items():
            yield Out(port=port, payload=out_df)
        self._reset_buffers()

    async def _process_bulk_mapping(
        self,
        dataframe: pd.DataFrame,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        srules = self._rules_as_tuples()
        for port, out_df in self._receiver.map_bulk(
            dataframe=dataframe,
            rules=srules,
            out_port_schemas=self.out_port_schemas,
            path_separator=self._schema_path_separator,
            metrics=metrics,
        ):
            yield Out(port=port, payload=out_df)

    async def process_bigdata(
        self,
        ddf: Union[dd.DataFrame, InTagged],
        *,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        # Dask joins also use tagged envelopes; plain DDF for mapping
        if self._is_tagged_join(ddf):
            async for out in self._process_bigdata_tagged_join(ddf, metrics):
                yield out
            return

        if isinstance(ddf, dd.DataFrame):
            async for out in self._process_bigdata_mapping(ddf, metrics):
                yield out

    async def _process_bigdata_tagged_join(
        self,
        ddf: InTagged,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        # Concatenate DDFs per port; Ellipsis closes the port
        if ddf.payload is Ellipsis:
            self._closed_ports.add(ddf.in_port)
        else:
            cur = self._big_buf.get(ddf.in_port)
            if cur is None:
                self._big_buf[ddf.in_port] = ddf.payload
            else:
                self._big_buf[ddf.in_port] = dd.concat([cur, ddf.payload])

        if not self._ready_to_join():
            return

        results = self._receiver.run_bigdata_joins(
            buffers=self._big_buf,
            join_plan=self.join_plan,
            out_port_schemas=self.out_port_schemas,
            path_separator=self._schema_path_separator,
            metrics=metrics,
        )
        for port, out_ddf in results.items():
            yield Out(port=port, payload=out_ddf)
        self._reset_buffers()

    async def _process_bigdata_mapping(
        self,
        ddf: dd.DataFrame,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        srules = self._rules_as_tuples()
        for port, out_ddf in self._receiver.map_bigdata(
            ddf=ddf,
            rules=srules,
            out_port_schemas=self.out_port_schemas,
            path_separator=self._schema_path_separator,
            metrics=metrics,
        ):
            yield Out(port=port, payload=out_ddf)

    def _is_tagged_join(self, obj: Any) -> bool:
        # Tagged envelope indicates join-mode input
        return bool(self.join_plan.steps) and isinstance(obj, InTagged)

    def _rules_as_tuples(self) -> List[StrRule]:
        # Convert validated rules to the tuple form used by the receiver
        return [(r.src_port, r.src_path, r.dst_port, r.dst_path) for r in self.rules]

    @staticmethod
    def _compute_required_ports(
        join_plan: JoinPlan,
        in_port_schemas: Dict[str, Schema],
    ) -> set[str]:
        # Required ports are those original inputs referenced by steps
        if not join_plan.steps:
            return set()
        ports: set[str] = set()
        in_ports = set(in_port_schemas.keys())
        for st in join_plan.steps:
            if st.left_port in in_ports:
                ports.add(st.left_port)
            if st.right_port in in_ports:
                ports.add(st.right_port)
        return ports

    def _ready_to_join(self) -> bool:
        # All required inputs must signal closure before we join
        return self._required_ports.issubset(self._closed_ports)

    def _reset_buffers(self) -> None:
        # Clear state so subsequent batches start fresh
        self._row_buf = {}
        self._bulk_buf = {}
        self._big_buf = {}
        self._closed_ports = set()
