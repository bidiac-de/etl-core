from __future__ import annotations

from typing import Any, AsyncIterator, Dict, List, Tuple, Union

import dask.dataframe as dd
import pandas as pd
from pydantic import Field, PrivateAttr, model_validator

from etl_core.components.component_registry import register_component
from etl_core.components.data_operations.data_operations import (
    DataOperationsComponent,
)
from etl_core.components.data_operations.schema_mapping.mapping_rule import (
    FieldMapping,
    FieldMappingSrc,
    Key,
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
from etl_core.components.envelopes import Out, InTagged, unwrap
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa: E501
    DataOperationsMetrics,
)

StrRule = Tuple[str, str, str, str]


@register_component("schema_mapping")
class SchemaMappingComponent(DataOperationsComponent):
    """
    Orchestrates schema mapping and joining according to defined rules.
    Config uses a nested dict for rules (rules_by_dest) which
    provides inherent uniqueness per destination field.
    """

    rules_by_dest: Dict[str, Dict[str, FieldMappingSrc]] = Field(default_factory=dict)
    join_plan: JoinPlan = Field(default_factory=JoinPlan)

    _receiver: SchemaMappingReceiver = PrivateAttr()
    _rules_map: Dict[Key, FieldMapping] = PrivateAttr(default_factory=dict)

    _row_buf: Dict[str, List[Dict[str, Any]]] = PrivateAttr(default_factory=dict)
    _bulk_buf: Dict[str, pd.DataFrame] = PrivateAttr(default_factory=dict)
    _big_buf: Dict[str, dd.DataFrame] = PrivateAttr(default_factory=dict)
    _closed_ports: set[str] = PrivateAttr(default_factory=set)
    _required_ports: set[str] = PrivateAttr(default_factory=set)

    @property
    def rules_effective(self) -> Dict[Key, FieldMapping]:
        """Read-only access to the validated canonical rules dict."""
        return self._rules_map

    @model_validator(mode="after")
    def _build_objects(self) -> "SchemaMappingComponent":
        # Fresh receiver instance
        self._receiver = SchemaMappingReceiver()

        # Validate mapping rules from nested JSON into canonical dict
        self._rules_map = validate_field_mappings(
            self.rules_by_dest,
            in_port_schemas=self.in_port_schemas,
            out_port_schemas=self.out_port_schemas,
            component_name=self.name,
            path_separator=self._schema_path_separator,
        )

        # Validate join plan
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
        row: Union[Dict[str, Any], InTagged],
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        # Normalize via
        in_port, payload = unwrap(row, default_port="")
        if payload is Ellipsis:
            self._closed_ports.add(in_port)
        else:
            if isinstance(payload, dict):
                self._row_buf.setdefault(in_port, []).append(payload)

        # Join once all required ports have been closed
        if not self._ready_to_join():
            return

        results = self._receiver.run_row_joins(
            buffers=self._row_buf,
            join_plan=self.join_plan,
            metrics=metrics,
        )
        for port, rows in results.items():
            for rec in rows:
                yield Out(port=port, payload=rec)
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
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        # Tagged envelopes for bulk joins: plain frames for mapping
        if self._is_tagged_join(dataframe):
            async for out in self._process_bulk_tagged_join(dataframe, metrics):
                yield out
            return

        if isinstance(dataframe, pd.DataFrame):
            async for out in self._process_bulk_mapping(dataframe, metrics):
                yield out

    async def _process_bulk_tagged_join(
        self,
        dataframe: Union[pd.DataFrame, InTagged],
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        in_port, payload = unwrap(dataframe, default_port="")
        if payload is Ellipsis:
            self._closed_ports.add(in_port)
        else:
            if isinstance(payload, pd.DataFrame):
                cur = self._bulk_buf.get(in_port)
                if cur is None:
                    self._bulk_buf[in_port] = payload
                else:
                    self._bulk_buf[in_port] = pd.concat(
                        [cur, payload], ignore_index=True
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
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        # Dask joins also use tagged envelopes: plain DDF for mapping
        if self._is_tagged_join(ddf):
            async for out in self._process_bigdata_tagged_join(ddf, metrics):
                yield out
            return

        if isinstance(ddf, dd.DataFrame):
            async for out in self._process_bigdata_mapping(ddf, metrics):
                yield out

    async def _process_bigdata_tagged_join(
        self,
        ddf: Union[dd.DataFrame, InTagged],
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        in_port, payload = unwrap(ddf, default_port="")
        if payload is Ellipsis:
            self._closed_ports.add(in_port)
        else:
            if isinstance(payload, dd.DataFrame):
                cur = self._big_buf.get(in_port)
                if cur is None:
                    self._big_buf[in_port] = payload
                else:
                    self._big_buf[in_port] = dd.concat([cur, payload])

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
        # Convert validated dict rules to the tuple form used by the receiver
        rules_map = self._rules_map
        return [
            (r.src_port, r.src_path, r.dst_port, r.dst_path) for r in rules_map.values()
        ]

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
        # All required inputs must signal closure before join
        return self._required_ports.issubset(self._closed_ports)

    def _reset_buffers(self) -> None:
        # Clear state so subsequent batches start fresh
        self._row_buf = {}
        self._bulk_buf = {}
        self._big_buf = {}
        self._closed_ports = set()
