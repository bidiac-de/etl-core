from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple

import dask.dataframe as dd
import pandas as pd
from etl_core.utils.common_helpers import get_leaf_field_map
from etl_core.components.data_operations.schema_mapping.join_rules import JoinPlan
from etl_core.components.wiring.schema import Schema
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa: E501
    DataOperationsMetrics,
)


class SchemaPath:
    """Parsed dotted path for nested dict access like 'user.address.city'"""

    __slots__ = ("parts",)

    def __init__(self, parts: List[str]) -> None:
        self.parts = parts

    @classmethod
    def parse(cls, text: str) -> "SchemaPath":
        parts = [p for p in text.split(".") if p]
        return cls(parts=parts)


StrRule = Tuple[str, str, str, str]


class SchemaMappingReceiver:
    """
    Worker class to implement schema mapping and join operations, called
    from the SchemaMapping Component.
    """

    def map_row(
        self,
        *,
        row: Dict[str, Any],
        rules: Iterable[StrRule],
        metrics: DataOperationsMetrics,
    ) -> Iterable[Tuple[str, Dict[str, Any]]]:
        # Count one incoming row for this mapping call
        metrics.lines_received += 1

        # Parse rule paths once to avoid repeated splitting
        compiled: List[Tuple[str, SchemaPath, str, SchemaPath]] = [
            (sp, SchemaPath.parse(ss), dp, SchemaPath.parse(ds))
            for sp, ss, dp, ds in rules
        ]

        # Group by destination port so one payload per port is emitted
        rules_by_dst: Dict[str, List[Tuple[SchemaPath, SchemaPath]]] = {}
        for _sp, src_p, dst_port, dst_p in compiled:
            rules_by_dst.setdefault(dst_port, []).append((src_p, dst_p))

        # Build outputs and account for processed and forwarded rows
        for dst_port, pairs in rules_by_dst.items():
            out: Dict[str, Any] = {}
            for src_p, dst_p in pairs:
                val = _read_path(row, src_p)
                _write_path(out, dst_p, val)
            if out:
                metrics.lines_processed += 1
                metrics.lines_forwarded += 1
                yield dst_port, out

    def map_bulk(
        self,
        *,
        dataframe: pd.DataFrame,
        rules: Iterable[StrRule],
        out_port_schemas: Dict[str, Schema],
        path_separator: str,
        metrics: DataOperationsMetrics,
    ) -> Iterable[Tuple[str, pd.DataFrame]]:
        # Count incoming rows from this batch
        metrics.lines_received += int(dataframe.shape[0])

        # Rules can target different output ports, handle each separately
        rules_by_dst: Dict[str, List[Tuple[str, str]]] = {}
        for _sp, src_path, dst_port, dst_path in rules:
            rules_by_dst.setdefault(dst_port, []).append((src_path, dst_path))

        for dst_port, pairs in rules_by_dst.items():
            out_df = _map_dataframe(dataframe, pairs)
            out_df = self._select_bulk_columns(
                out_df, dst_port, out_port_schemas, path_separator
            )
            lines = int(out_df.shape[0])
            metrics.lines_processed += lines
            metrics.lines_forwarded += lines
            yield dst_port, out_df

    def map_bigdata(
        self,
        *,
        ddf: "dd.DataFrame",
        rules: Iterable[StrRule],
        out_port_schemas: Dict[str, Schema],
        path_separator: str,
        metrics: DataOperationsMetrics,
    ) -> Iterable[Tuple[str, "dd.DataFrame"]]:
        # Count rows defensively, graph may not be immediately evaluable
        in_len = self._safe_ddf_len(ddf)
        metrics.lines_received += in_len

        # Same grouping as bulk, apply per partition
        rules_by_dst: Dict[str, List[Tuple[str, str]]] = {}
        for _sp, src_path, dst_port, dst_path in rules:
            rules_by_dst.setdefault(dst_port, []).append((src_path, dst_path))

        for dst_port, pairs in rules_by_dst.items():
            meta = _infer_meta_from_pairs(ddf, pairs)
            out_ddf = ddf.map_partitions(_map_dataframe, pairs, meta=meta)
            out_ddf = self._select_big_columns(
                out_ddf, dst_port, out_port_schemas, path_separator
            )
            out_len = self._safe_ddf_len(out_ddf)
            metrics.lines_processed += out_len
            metrics.lines_forwarded += out_len
            yield dst_port, out_ddf

    def run_row_joins(
        self,
        *,
        buffers: Dict[str, List[Dict[str, Any]]],
        join_plan: JoinPlan,
        metrics: DataOperationsMetrics,
    ) -> Dict[str, List[Dict[str, Any]]]:
        # Count all buffered inputs that participate in this join
        metrics.lines_received += sum(len(v) for v in buffers.values())

        # Work on a copy, each step can feed later steps via its port
        working: Dict[str, List[Dict[str, Any]]] = {
            k: list(v) for k, v in buffers.items()
        }
        for step in join_plan.steps:
            joined = self._join_rows_for_step(
                working=working,
                left_port=step.left_port,
                right_port=step.right_port,
                left_on=step.left_on,
                right_on=step.right_on,
                how=step.how,
            )
            working[step.output_port] = joined

        results = {
            s.output_port: working.get(s.output_port, []) for s in join_plan.steps
        }  # noqa: E501

        # Account for emitted rows across all output ports
        out_count = sum(len(rows) for rows in results.values())
        metrics.lines_processed += out_count
        metrics.lines_forwarded += out_count
        return results

    def run_bulk_joins(
        self,
        *,
        buffers: Dict[str, pd.DataFrame],
        join_plan: JoinPlan,
        out_port_schemas: Dict[str, Schema],
        path_separator: str,
        metrics: DataOperationsMetrics,
    ) -> Dict[str, pd.DataFrame]:
        # Count buffered rows for all inputs
        metrics.lines_received += sum(int(df.shape[0]) for df in buffers.values())

        dfs: Dict[str, pd.DataFrame] = dict(buffers)
        for step in join_plan.steps:
            left = dfs.get(step.left_port, pd.DataFrame())
            right = dfs.get(step.right_port, pd.DataFrame())
            out = left.merge(
                right,
                how=step.how,
                left_on=step.left_on,
                right_on=step.right_on,
            )
            out = self._select_bulk_columns(
                out, step.output_port, out_port_schemas, path_separator
            )
            dfs[step.output_port] = out

        results: Dict[str, pd.DataFrame] = {
            s.output_port: dfs.get(s.output_port, pd.DataFrame())
            for s in join_plan.steps
        }

        out_count = sum(int(df.shape[0]) for df in results.values())
        metrics.lines_processed += out_count
        metrics.lines_forwarded += out_count
        return results

    def run_bigdata_joins(
        self,
        *,
        buffers: Dict[str, dd.DataFrame],
        join_plan: JoinPlan,
        out_port_schemas: Dict[str, Schema],
        path_separator: str,
        metrics: DataOperationsMetrics,
    ) -> Dict[str, dd.DataFrame]:
        # Count buffered DDF rows defensively
        metrics.lines_received += sum(self._safe_ddf_len(v) for v in buffers.values())

        dfs: Dict[str, dd.DataFrame] = dict(buffers)

        for step in join_plan.steps:
            left = dfs.get(step.left_port)
            right = dfs.get(step.right_port)

            # Ensure both sides exist so merge is safe
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
            out = self._select_big_columns(
                out, step.output_port, out_port_schemas, path_separator
            )
            dfs[step.output_port] = out

        results = {s.output_port: dfs.get(s.output_port) for s in join_plan.steps}
        out_count = sum(
            self._safe_ddf_len(v) for v in results.values() if v is not None
        )  # noqa: E501
        metrics.lines_processed += out_count
        metrics.lines_forwarded += out_count
        return results

    @staticmethod
    def _select_bulk_columns(
        df: pd.DataFrame,
        out_port: str,
        out_port_schemas: Dict[str, Schema],
        path_separator: str,
    ) -> pd.DataFrame:
        """Trim to schema-defined leaf fields (full dotted names preserved)"""
        schema = out_port_schemas.get(out_port)
        if not isinstance(schema, Schema):
            return df
        leaf_map = get_leaf_field_map(schema, path_separator=path_separator)
        desired = list(leaf_map.keys())
        keep = [c for c in desired if c in df.columns]
        return df if not keep else df[keep]

    @staticmethod
    def _select_big_columns(
        ddf: "dd.DataFrame",
        out_port: str,
        out_port_schemas: Dict[str, Schema],
        path_separator: str,
    ) -> "dd.DataFrame":
        """Trim to schema-defined leaf fields (full dotted names preserved)"""
        schema = out_port_schemas.get(out_port)
        if not isinstance(schema, Schema):
            return ddf
        leaf_map = get_leaf_field_map(schema, path_separator=path_separator)
        desired = list(leaf_map.keys())
        keep = [c for c in desired if c in ddf.columns]
        return ddf if not keep else ddf[keep]

    @staticmethod
    def _dict_key_by_path(data: Dict[str, Any], dotted: str) -> Any:
        # Read a value from a nested dict by a dotted path
        path = SchemaPath.parse(dotted)
        cur: Any = data
        for part in path.parts:
            if not isinstance(cur, dict) or part not in cur:
                return None
            cur = cur[part]
        return cur

    def _index_rows_by_key(
        self,
        rows: List[Dict[str, Any]],
        dotted: str,
    ) -> Dict[Any, List[Dict[str, Any]]]:
        # Build index key -> list of rows to support one-to-many matches
        ix: Dict[Any, List[Dict[str, Any]]] = {}
        for r in rows:
            k = self._dict_key_by_path(r, dotted)
            ix.setdefault(k, []).append(r)
        return ix

    @staticmethod
    def _merge_nested_dicts(
        a: Dict[str, Any],
        b: Dict[str, Any],
    ) -> Dict[str, Any]:
        # Shallow merge, dicts are combined at the first level
        merged = dict(a)
        for k, v in b.items():
            if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
                merged[k] = {**merged[k], **v}
            else:
                merged[k] = v
        return merged

    def _join_rows_for_step(
        self,
        *,
        working: Dict[str, List[Dict[str, Any]]],
        left_port: str,
        right_port: str,
        left_on: str,
        right_on: str,
        how: str,
    ) -> List[Dict[str, Any]]:
        left_rows = working.get(left_port, [])
        right_rows = working.get(right_port, [])

        # Index both sides by the join key (dotted lookup)
        il = self._index_rows_by_key(left_rows, left_on)
        ir = self._index_rows_by_key(right_rows, right_on)

        left_keys = set(il.keys())
        right_keys = set(ir.keys())
        common = left_keys & right_keys
        left_only = left_keys - right_keys
        right_only = right_keys - left_keys

        result: List[Dict[str, Any]] = []

        # For common keys, produce the cross-product and merge dicts
        for k in common:
            for lrow in il[k]:
                for rrow in ir[k]:
                    result.append(self._merge_nested_dicts(lrow, rrow))

        # Include non-matches based on join type
        if how in ("left", "outer"):
            for k in left_only:
                result.extend(il[k])
        if how in ("right", "outer"):
            for k in right_only:
                result.extend(ir[k])
        return result

    @staticmethod
    def _safe_ddf_len(ddf: dd.DataFrame) -> int:
        # Compute row count defensively; some Dask graphs can fail here
        try:
            return int(ddf.map_partitions(len).sum().compute())
        except Exception:
            return 0


def _read_path(src: Dict[str, Any], path: SchemaPath) -> Any:
    cur: Any = src
    for key in path.parts:
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur


def _write_path(dst: Dict[str, Any], path: SchemaPath, value: Any) -> None:
    cur = dst
    for key in path.parts[:-1]:
        nxt = cur.get(key)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[key] = nxt
        cur = nxt
    if path.parts:
        cur[path.parts[-1]] = value


def _map_dataframe(
    df: pd.DataFrame,
    pairs: List[Tuple[str, str]],
) -> pd.DataFrame:
    # Build new frame with destination names, missing columns become None
    out_cols: Dict[str, Any] = {}
    for src_col, dst_col in pairs:
        out_cols[dst_col] = df[src_col] if src_col in df.columns else None
    return pd.DataFrame(out_cols, index=df.index)


def _infer_meta_from_pairs(
    ddf: dd.DataFrame,
    pairs: List[Tuple[str, str]],
) -> pd.DataFrame:
    """
    Build a zero-row pandas.DataFrame that matches the post-mapping schema.
    Each column is an empty Series with the correct dtype so Dask can
    infer the partition schema without computing.
    """
    cols: Dict[str, pd.Series] = {}
    for src_col, dst_col in pairs:
        if src_col in ddf.columns:
            dtype = ddf.dtypes[src_col]
            cols[dst_col] = pd.Series([], dtype=dtype)
        else:
            cols[dst_col] = pd.Series([], dtype="object")
    meta = pd.DataFrame(cols)
    return meta.iloc[:0]
