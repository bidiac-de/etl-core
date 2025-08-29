from __future__ import annotations

from typing import Any, AsyncIterator, Dict, Iterable, List, Tuple, Union

import dask.dataframe as dd
import pandas as pd

from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa: E501
    DataOperationsMetrics,
)
from etl_core.receivers.base_receiver import Receiver


class schema_path:
    """Parsed dotted path for nested dict access like 'user.address.city'."""

    __slots__ = ("parts",)

    def __init__(self, parts: List[str]) -> None:
        self.parts = parts

    @classmethod
    def parse(cls, text: str) -> "schema_path":
        parts = [p for p in text.split(".") if p]
        return cls(parts=parts)


# Rules can be string-based (bulk/bigdata) or compiled (row)
_StrRule = Tuple[str, str, str, str]
_CompiledRule = Tuple[str, schema_path, str, schema_path]
_Rule = Union[_StrRule, _CompiledRule]


class SchemaMappingReceiver(Receiver):
    """
    Receiver that performs schema mapping (fan-in / fan-out).
    It yields (dst_port, payload) tuples, for the component to
    build Out envelopes.

    New:
    - Accepts compiled _Path rules for row mode to avoid reparsing per row
    """

    async def process_row(
        self,
        row: Dict[str, Any],
        *,
        metrics: DataOperationsMetrics,
        rules: Iterable[_Rule],
    ) -> AsyncIterator[Tuple[str, Dict[str, Any]]]:
        metrics.lines_received += 1

        # Group rules by dst_port, for fan out and collecting multiple inputs
        rules_by_dst: Dict[
            str, List[Tuple[Union[str, schema_path], Union[str, schema_path]]]
        ] = {}
        for _src_port, src_path, dst_port, dst_path in rules:
            rules_by_dst.setdefault(dst_port, []).append((src_path, dst_path))

        # apply mapping rules
        for dst_port, pairs in rules_by_dst.items():
            out: Dict[str, Any] = {}
            for src_path, dst_path in pairs:
                src_p = (
                    src_path
                    if isinstance(src_path, schema_path)
                    else schema_path.parse(src_path)
                )  # cached for compiled
                dst_p = (
                    dst_path
                    if isinstance(dst_path, schema_path)
                    else schema_path.parse(dst_path)
                )
                val = _read_path(row, src_p)
                _write_path(out, dst_p, val)
            if out:
                metrics.lines_processed += 1
                metrics.lines_forwarded += 1
                yield dst_port, out

    async def process_bulk(
        self,
        dataframe: pd.DataFrame,
        *,
        metrics: DataOperationsMetrics,
        rules: Iterable[_Rule],
    ) -> AsyncIterator[Tuple[str, pd.DataFrame]]:
        metrics.lines_received += int(dataframe.shape[0])

        # Group rules by dst_port, string column names
        rules_by_dst: Dict[str, List[Tuple[str, str]]] = {}
        for _src_port, src_path, dst_port, dst_path in rules:
            src = src_path.parts[-1] if isinstance(src_path, schema_path) else src_path
            dst = dst_path.parts[-1] if isinstance(dst_path, schema_path) else dst_path
            rules_by_dst.setdefault(dst_port, []).append((src, dst))

        # apply mapping rules
        for dst_port, pairs in rules_by_dst.items():
            out_df = _map_dataframe(dataframe, pairs)
            lines = int(out_df.shape[0])
            metrics.lines_processed += lines
            metrics.lines_forwarded += lines
            yield dst_port, out_df

    async def process_bigdata(
        self,
        ddf: dd.DataFrame,
        *,
        metrics: DataOperationsMetrics,
        rules: Iterable[_Rule],
    ) -> AsyncIterator[Tuple[str, dd.DataFrame]]:
        try:
            rows_in = int(ddf.map_partitions(len).sum().compute())
        except Exception:
            rows_in = 0
        metrics.lines_received += rows_in

        # Group rules by dst_port
        rules_by_dst: Dict[str, List[Tuple[str, str]]] = {}
        for _src_port, src_path, dst_port, dst_path in rules:
            src = src_path.parts[-1] if isinstance(src_path, schema_path) else src_path
            dst = dst_path.parts[-1] if isinstance(dst_path, schema_path) else dst_path
            rules_by_dst.setdefault(dst_port, []).append((src, dst))

        # apply mapping rules
        for dst_port, pairs in rules_by_dst.items():
            out_ddf = ddf.map_partitions(
                lambda part, _pairs=pairs: _map_dataframe(part, _pairs),
                meta=_infer_meta_from_pairs(ddf, pairs),
            )

            try:
                rows_out = int(out_ddf.map_partitions(len).sum().compute())
            except Exception:
                rows_out = 0
            metrics.lines_processed += rows_out
            metrics.lines_forwarded += rows_out

            yield dst_port, out_ddf


# helpers


def _read_path(src: Dict[str, Any], path: schema_path) -> Any:
    cur: Any = src
    for key in path.parts:
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur


def _write_path(dst: Dict[str, Any], path: schema_path, value: Any) -> None:
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
    """Create a new DataFrame with columns mapped via (src_col, dst_col)."""
    out_cols: Dict[str, Any] = {}
    for src_col, dst_col in pairs:
        out_cols[dst_col] = df[src_col] if src_col in df.columns else None
    return pd.DataFrame(out_cols, index=df.index)


def _infer_meta_from_pairs(
    ddf: dd.DataFrame,
    pairs: List[Tuple[str, str]],
) -> pd.DataFrame:
    cols: Dict[str, Any] = {}
    for src_col, dst_col in pairs:
        if src_col in ddf.columns:
            cols[dst_col] = ddf._meta[src_col]
        else:
            cols[dst_col] = pd.Series([], dtype="object")
    return pd.DataFrame(cols)
