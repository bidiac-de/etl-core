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
from etl_core.components.envelopes import InTagged, Out
from etl_core.components.component_registry import register_component
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa: E501
    DataOperationsMetrics,
)
from etl_core.receivers.data_operations_receivers.schema_mapping.schema_mapping_receiver import (  # noqa: E501
    SchemaMappingReceiver,
    schema_path,
)
from etl_core.components.wiring.schema import Schema
from etl_core.utils.common_helpers import get_leaf_field_map


CompiledRule = Tuple[str, schema_path, str, schema_path]
StrRule = Tuple[str, str, str, str]


@register_component("schema_mapping")
class SchemaMappingComponent(DataOperationsComponent):
    """
    Schema mapping with optional multi-step joins and internal buffering.

    - Single-input or no join_plan -> behaves like before (mapping only).
    - Multi-input with join_plan   -> expects InTagged items to arrive:
        * regular data: InTagged(in_port, payload)
        * end-of-port: InTagged(in_port, Ellipsis)  (created by worker)
      When each required input port has emitted Ellipsis, execute the plan.
    """

    rules: List[FieldMapping] = Field(default_factory=list)
    join_plan: JoinPlan = Field(default_factory=JoinPlan)

    _receiver: SchemaMappingReceiver = PrivateAttr()
    _compiled_row_rules: List[CompiledRule] = PrivateAttr(default=[])
    _string_rules: List[StrRule] = PrivateAttr(default=[])

    # join state
    _row_buf: Dict[str, List[Dict[str, Any]]] = PrivateAttr(default_factory=dict)
    _bulk_buf: Dict[str, pd.DataFrame] = PrivateAttr(default_factory=dict)
    _big_buf: Dict[str, dd.DataFrame] = PrivateAttr(default_factory=dict)
    _closed_ports: set[str] = PrivateAttr(default_factory=set)
    _required_ports: set[str] = PrivateAttr(default_factory=set)

    @model_validator(mode="after")
    def _build_objects(self) -> "SchemaMappingComponent":
        self._receiver = SchemaMappingReceiver()

        # 1) validate rules and plan in their own modules
        validate_field_mappings(
            self.rules,
            in_port_schemas=self.in_port_schemas,
            out_port_schemas=self.out_port_schemas,
            component_name=self.name,
            path_separator=self._schema_path_separator,
        )
        validate_join_plan(
            self.join_plan,
            in_port_schemas=self.in_port_schemas,
            out_port_names={p.name for p in self.expected_ports()},
            component_name=self.name,
            path_separator=self._schema_path_separator,
        )

        # 2) pre-compile rules for fast execution
        self._compile_rules()

        # 3) compute required ports for joining
        self._required_ports = self._compute_required_ports()
        return self

    def requires_tagged_input(self) -> bool:
        """
        Worker uses this to decide whether to pass InTagged through unchanged.
        """
        return bool(self.join_plan.steps) and len(self.expected_in_port_names()) > 1

    async def process_row(
        self,
        row: Union[Dict[str, Any], InTagged],
        *,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        if self.join_plan.steps and isinstance(row, InTagged):
            if row.payload is Ellipsis:
                self._closed_ports.add(row.in_port)
            else:
                self._row_buf.setdefault(row.in_port, []).append(row.payload)
            if self._ready_to_join():
                async for out in self._run_row_joins(metrics):
                    yield out
            return

        async for port, payload in self._receiver.process_row(
            row, metrics=metrics, rules=self._compiled_row_rules
        ):
            yield Out(port=port, payload=payload)

    async def process_bulk(
        self,
        dataframe: Union[pd.DataFrame, InTagged],
        *,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        if self.join_plan.steps and isinstance(dataframe, InTagged):
            if dataframe.payload is Ellipsis:
                self._closed_ports.add(dataframe.in_port)
            else:
                cur = self._bulk_buf.get(dataframe.in_port)
                if cur is None:
                    self._bulk_buf[dataframe.in_port] = dataframe.payload
                else:
                    self._bulk_buf[dataframe.in_port] = pd.concat(
                        [cur, dataframe.payload], ignore_index=True
                    )
            if self._ready_to_join():
                async for out in self._run_bulk_joins(metrics):
                    yield out
            return

        async for port, payload in self._receiver.process_bulk(
            dataframe, metrics=metrics, rules=self._string_rules
        ):
            yield Out(port=port, payload=payload)

    async def process_bigdata(
        self,
        ddf: Union[dd.DataFrame, InTagged],
        *,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        if self.join_plan.steps and isinstance(ddf, InTagged):
            if ddf.payload is Ellipsis:
                self._closed_ports.add(ddf.in_port)
            else:
                cur = self._big_buf.get(ddf.in_port)
                if cur is None:
                    self._big_buf[ddf.in_port] = ddf.payload
                else:
                    self._big_buf[ddf.in_port] = dd.concat([cur, ddf.payload])
            if self._ready_to_join():
                async for out in self._run_bigdata_joins(metrics):
                    yield out
            return

        async for port, payload in self._receiver.process_bigdata(
            ddf, metrics=metrics, rules=self._string_rules
        ):
            yield Out(port=port, payload=payload)

    def _compile_rules(self) -> None:
        # keep compilation local so receiver can work with fast paths
        if not self.rules:
            self._compiled_row_rules = []
            self._string_rules = []
            return

        self._compiled_row_rules = [
            (
                r.src_port,
                schema_path.parse(r.src_path),
                r.dst_port,
                schema_path.parse(r.dst_path),
            )
            for r in self.rules
        ]
        self._string_rules = [
            (r.src_port, r.src_path, r.dst_port, r.dst_path) for r in self.rules
        ]

    def _select_bulk_columns(self, df: pd.DataFrame, out_port: str) -> pd.DataFrame:
        """Project a pandas DF to the columns defined by the out port schema."""
        schema = self.out_port_schemas.get(out_port)
        if not isinstance(schema, Schema):
            return df
        leaf_map = get_leaf_field_map(schema, path_separator=".")
        desired = [p.split(".")[-1] for p in leaf_map.keys()]
        keep = [c for c in desired if c in df.columns]
        return df if not keep else df[keep]

    def _select_big_columns(self, ddf: dd.DataFrame, out_port: str) -> dd.DataFrame:
        """Project a Dask DF to the columns defined by the out port schema."""
        schema = self.out_port_schemas.get(out_port)
        if not isinstance(schema, Schema):
            return ddf
        leaf_map = get_leaf_field_map(schema, path_separator=".")
        desired = [p.split(".")[-1] for p in leaf_map.keys()]
        keep = [c for c in desired if c in ddf.columns]
        return ddf if not keep else ddf[keep]

    def _ready_to_join(self) -> bool:
        return self._required_ports.issubset(self._closed_ports)

    def _compute_required_ports(self) -> set[str]:
        if not self.join_plan.steps:
            return set()
        ports: set[str] = set()
        for st in self.join_plan.steps:
            if st.left_port in self.in_port_schemas:
                ports.add(st.left_port)
            if st.right_port in self.in_port_schemas:
                ports.add(st.right_port)
        return ports

    @staticmethod
    def _dict_key_by_path(data: Dict[str, Any], dotted: str) -> Any:
        """Extract nested value by dotted path; None if any segment is missing."""
        path = schema_path.parse(dotted)
        cur: Any = data
        for part in path.parts:
            if not isinstance(cur, dict) or part not in cur:
                return None
            cur = cur[part]
        return cur

    def _index_rows_by_key(
        self, rows: List[Dict[str, Any]], dotted: str
    ) -> Dict[Any, List[Dict[str, Any]]]:
        """Build an index: key -> list of rows sharing that key."""
        ix: Dict[Any, List[Dict[str, Any]]] = {}
        for r in rows:
            k = self._dict_key_by_path(r, dotted)
            ix.setdefault(k, []).append(r)
        return ix

    @staticmethod
    def _merge_nested_dicts(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
        """
        Shallow merge with one-level nested dict coalescing.
        Values in `b` override/extend those in `a`.
        """
        merged = dict(a)
        for k, v in b.items():
            if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
                merged[k] = {**merged[k], **v}
            else:
                merged[k] = v
        return merged

    def _join_rows_for_step(
        self,
        working: Dict[str, List[Dict[str, Any]]],
        *,
        left_port: str,
        right_port: str,
        left_on: str,
        right_on: str,
        how: str,
    ) -> List[Dict[str, Any]]:
        """
        Perform one join step between two lists of dict rows.
        """
        left_rows = working.get(left_port, [])
        right_rows = working.get(right_port, [])

        il = self._index_rows_by_key(left_rows, left_on)
        ir = self._index_rows_by_key(right_rows, right_on)

        left_keys = set(il.keys())
        right_keys = set(ir.keys())
        common_keys = left_keys & right_keys
        left_only = left_keys - right_keys
        right_only = right_keys - left_keys

        result: List[Dict[str, Any]] = []

        for k in common_keys:
            ls = il[k]
            rs = ir[k]
            for left in ls:
                for r in rs:
                    result.append(self._merge_nested_dicts(left, r))

        if how in ("left", "outer"):
            for k in left_only:
                result.extend(il[k])

        if how in ("right", "outer"):
            for k in right_only:
                result.extend(ir[k])

        return result

    async def _emit_join_results(
        self,
        working: Dict[str, List[Dict[str, Any]]],
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        """Emit results for each output port exactly once, update metrics."""
        emitted: set[str] = set()
        for st in self.join_plan.steps:
            if st.output_port in emitted:
                continue
            emitted.add(st.output_port)
            rows = working.get(st.output_port, [])
            for r in rows:
                metrics.lines_processed += 1
                metrics.lines_forwarded += 1
                yield Out(port=st.output_port, payload=r)

    async def _run_row_joins(
        self,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        """
        Execute multi-step joins in row mode using buffered per-port rows.
        """
        working: Dict[str, List[Dict[str, Any]]] = {
            k: list(v) for k, v in self._row_buf.items()
        }

        for step in self.join_plan.steps:
            joined = self._join_rows_for_step(
                working,
                left_port=step.left_port,
                right_port=step.right_port,
                left_on=step.left_on,
                right_on=step.right_on,
                how=step.how,
            )
            working[step.output_port] = joined

        async for out in self._emit_join_results(working, metrics):
            yield out

        self._reset_buffers()

    async def _run_bulk_joins(
        self,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        dfs: Dict[str, pd.DataFrame] = {}
        for k, v in self._bulk_buf.items():
            dfs[k] = v

        for step in self.join_plan.steps:
            left = dfs.get(step.left_port, pd.DataFrame())
            right = dfs.get(step.right_port, pd.DataFrame())
            out = left.merge(
                right,
                how=step.how,
                left_on=step.left_on,
                right_on=step.right_on,
            )
            out = self._select_bulk_columns(out, step.output_port)
            dfs[step.output_port] = out

        emitted: set[str] = set()
        for st in self.join_plan.steps:
            if st.output_port in emitted:
                continue
            emitted.add(st.output_port)
            df = dfs.get(st.output_port, pd.DataFrame())
            lines = int(df.shape[0])
            metrics.lines_processed += lines
            metrics.lines_forwarded += lines
            yield Out(port=st.output_port, payload=df)

        self._reset_buffers()

    async def _run_bigdata_joins(
        self,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        dfs: Dict[str, dd.DataFrame] = {}
        for k, v in self._big_buf.items():
            dfs[k] = v

        for step in self.join_plan.steps:
            left = dfs.get(step.left_port)
            right = dfs.get(step.right_port)

            if left is None and right is None:
                dfs[step.output_port] = dd.from_pandas(pd.DataFrame(), npartitions=1)
                continue
            if left is None:
                left = dd.from_pandas(
                    pd.DataFrame(columns=[step.left_on]), npartitions=1
                )
            if right is None:
                right = dd.from_pandas(
                    pd.DataFrame(columns=[step.right_on]), npartitions=1
                )

            out = left.merge(
                right,
                how=step.how,
                left_on=step.left_on,
                right_on=step.right_on,
            )
            out = self._select_big_columns(out, step.output_port)
            dfs[step.output_port] = out

        emitted: set[str] = set()
        for st in self.join_plan.steps:
            if st.output_port in emitted:
                continue
            emitted.add(st.output_port)
            ddf = dfs.get(st.output_port)
            if ddf is None:
                continue
            try:
                rows_out = int(ddf.map_partitions(len).sum().compute())
            except Exception:
                rows_out = 0
            metrics.lines_processed += rows_out
            metrics.lines_forwarded += rows_out
            yield Out(port=st.output_port, payload=ddf)

        self._reset_buffers()

    def _reset_buffers(self) -> None:
        self._row_buf.clear()
        self._bulk_buf.clear()
        self._big_buf.clear()
        self._closed_ports.clear()
