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
)
from etl_core.components.data_operations.schema_mapping.join_rules import (
    JoinPlan,
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
from etl_core.components.wiring.column_definition import DataType, FieldDef
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
        self._validate_and_prepare_rules()
        self._validate_join_plan()
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

    def _select_bulk_columns(self, df: pd.DataFrame, out_port: str) -> pd.DataFrame:
        """Project a pandas DF to the columns defined by the out port schema."""
        schema = self.out_port_schemas.get(out_port)
        if not isinstance(schema, Schema):
            return df
        # flatten schema leaf paths and keep last segment as column name
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

    async def _run_row_joins(
        self,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        working: Dict[str, List[Dict[str, Any]]] = {
            k: list(v) for k, v in self._row_buf.items()
        }

        def key_of(d: Dict[str, Any], dotted: str) -> Any:
            p = schema_path.parse(dotted)
            cur: Any = d
            for part in p.parts:
                if not isinstance(cur, dict) or part not in cur:
                    return None
                cur = cur[part]
            return cur

        def index(
            rows: List[Dict[str, Any]], dotted: str
        ) -> Dict[Any, List[Dict[str, Any]]]:
            ix: Dict[Any, List[Dict[str, Any]]] = {}
            for r in rows:
                k = key_of(r, dotted)
                ix.setdefault(k, []).append(r)
            return ix

        for step in self.join_plan.steps:
            left_rows = working.get(step.left_port, [])
            right_rows = working.get(step.right_port, [])
            il = index(left_rows, step.left_on)
            ir = index(right_rows, step.right_on)

            result: List[Dict[str, Any]] = []
            left_keys = set(il.keys())
            right_keys = set(ir.keys())
            keys_inner = left_keys & right_keys

            def merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
                merged = dict(a)
                for k, v in b.items():
                    if (
                        k in merged
                        and isinstance(merged[k], dict)
                        and isinstance(v, dict)
                    ):
                        merged[k] = {**merged[k], **v}
                    else:
                        merged[k] = v
                return merged

            if step.how in ("inner", "left", "outer"):
                keys = (
                    keys_inner
                    if step.how == "inner"
                    else left_keys if step.how == "left" else left_keys | right_keys
                )
                for k in keys:
                    ls = il.get(k, [])
                    rs = ir.get(k, [])
                    if ls and rs:
                        for left in ls:
                            for r in rs:
                                result.append(merge(left, r))
                    elif step.how in ("left", "outer") and ls and not rs:
                        result.extend(ls)

            if step.how in ("right", "outer"):
                only_right = right_keys - left_keys
                for k in only_right:
                    result.extend(ir.get(k, []))

            working[step.output_port] = result

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

        self._reset_buffers()

    async def _run_bulk_joins(
        self,
        metrics: DataOperationsMetrics,
    ) -> AsyncIterator[Out]:
        dfs: Dict[str, pd.DataFrame] = {k: v for k, v in self._bulk_buf.items()}
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
        dfs: Dict[str, dd.DataFrame] = {k: v for k, v in self._big_buf.items()}
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

    def _validate_and_prepare_rules(self) -> None:
        if not self.rules:
            self._compiled_row_rules = []
            self._string_rules = []
            return

        seen: set[tuple[str, str]] = set()
        for r in self.rules:
            key = (r.dst_port, r.dst_path)
            if key in seen:
                raise ValueError(
                    f"{self.name}: duplicate mapping to destination {key!r}"
                )
            seen.add(key)

        for r in self.rules:
            src_schema = self.schema_for_in_port(r.src_port)
            if not isinstance(src_schema, Schema):
                raise ValueError(
                    f"{self.name}: no schema for input port {r.src_port!r}"
                )
            dst_schema = self.schema_for_out_port(r.dst_port)
            if not isinstance(dst_schema, Schema):
                raise ValueError(
                    f"{self.name}: no schema for output port {r.dst_port!r}"
                )

            src_map = get_leaf_field_map(src_schema, self._schema_path_separator)
            dst_map = get_leaf_field_map(dst_schema, self._schema_path_separator)

            src_fd = src_map.get(r.src_path)
            if src_fd is None:
                raise ValueError(
                    f"{self.name}: unknown source path "
                    f"{r.src_port!r}:{r.src_path!r}"
                )
            dst_fd = dst_map.get(r.dst_path)
            if dst_fd is None:
                raise ValueError(
                    f"{self.name}: unknown destination path "
                    f"{r.dst_port!r}:{r.dst_path!r}"
                )
            if not self._types_compatible(src_fd, dst_fd):
                raise ValueError(
                    f"{self.name}: type mismatch {r.src_port}:{r.src_path} "
                    f"({src_fd.data_type}) -> {r.dst_port}:{r.dst_path} "
                    f"({dst_fd.data_type})"
                )

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

    def _validate_join_plan(self) -> None:
        if not self.join_plan.steps:
            return

        in_ports = set(self.expected_in_port_names())
        out_ports = {p.name for p in self.expected_ports()}

        for step in self.join_plan.steps:
            if step.left_port not in in_ports and step.left_port not in out_ports:
                raise ValueError(f"{self.name}: unknown left_port {step.left_port!r}")
            if step.right_port not in in_ports and step.right_port not in out_ports:
                raise ValueError(f"{self.name}: unknown right_port {step.right_port!r}")
            if step.output_port not in out_ports:
                raise ValueError(
                    f"{self.name}: unknown output_port {step.output_port!r}"
                )

            def _check_key(port_name: str, key: str) -> None:
                schema = self.in_port_schemas.get(port_name)
                if schema is None:
                    return
                leafs = get_leaf_field_map(schema, self._schema_path_separator)
                if key not in leafs:
                    raise ValueError(
                        f"{self.name}: join key {key!r} not in schema for "
                        f"port {port_name!r}"
                    )

            _check_key(step.left_port, step.left_on)
            _check_key(step.right_port, step.right_on)

    @staticmethod
    def _types_compatible(src: FieldDef, dst: FieldDef) -> bool:
        def norm(dt: DataType) -> DataType:
            return DataType.STRING if dt == DataType.PATH else dt

        return norm(src.data_type) == norm(dst.data_type)
