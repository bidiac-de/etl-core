from __future__ import annotations

from typing import Any, AsyncIterator, Dict, Iterable, List, Tuple

import dask.dataframe as dd
import pandas as pd

from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa: E501
    DataOperationsMetrics,
)
from etl_core.receivers.base_receiver import Receiver


class _Path:
    """Parsed dotted path for nested dict access like 'user.address.city'."""

    __slots__ = ("parts",)

    def __init__(self, parts: List[str]) -> None:
        self.parts = parts

    @classmethod
    def parse(cls, text: str) -> "_Path":
        parts = [p for p in text.split(".") if p]
        return cls(parts=parts)


class SchemaMappingReceiver(Receiver):
    """
    Receiver that performs schema mapping (fan-in / fan-out).
    It yields (dst_port, payload) tuples, for the component to
    build Out envelopes.
    """

    async def process_row(
        self,
        row: Dict[str, Any],
        *,
        metrics: DataOperationsMetrics,
        rules: Iterable[Tuple[str, str, str, str]],
    ) -> AsyncIterator[Tuple[str, Dict[str, Any]]]:
        metrics.lines_received += 1

        # Group rules by dst_port, for fan out and collecting multiple inputs
        rules_by_dst: Dict[str, List[Tuple[str, str]]] = {}
        for _src_port, src_path, dst_port, dst_path in rules:
            rules_by_dst.setdefault(dst_port, []).append((src_path, dst_path))

        # apply mapping rules
        for dst_port, pairs in rules_by_dst.items():
            out: Dict[str, Any] = {}
            for src_path, dst_path in pairs:
                val = _read_path(row, _Path.parse(src_path))
                _write_path(out, _Path.parse(dst_path), val)
            if out:
                metrics.lines_processed += 1
                metrics.lines_forwarded += 1
                yield dst_port, out

    async def process_bulk(
        self,
        dataframe: pd.DataFrame,
        *,
        metrics: DataOperationsMetrics,
        rules: Iterable[Tuple[str, str, str, str]],
    ) -> AsyncIterator[Tuple[str, pd.DataFrame]]:
        metrics.lines_received += int(dataframe.shape[0])

        # Group rules by dst_port
        rules_by_dst: Dict[str, List[Tuple[str, str]]] = {}
        for _src_port, src_path, dst_port, dst_path in rules:
            rules_by_dst.setdefault(dst_port, []).append((src_path, dst_path))

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
        rules: Iterable[Tuple[str, str, str, str]],
    ) -> AsyncIterator[Tuple[str, dd.DataFrame]]:
        try:
            rows_in = int(ddf.map_partitions(len).sum().compute())
        except Exception:
            rows_in = 0
        metrics.lines_received += rows_in

        # Group rules by dst_port
        rules_by_dst: Dict[str, List[Tuple[str, str]]] = {}
        for _src_port, src_path, dst_port, dst_path in rules:
            rules_by_dst.setdefault(dst_port, []).append((src_path, dst_path))

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


def _read_path(src: Dict[str, Any], path: _Path) -> Any:
    cur: Any = src
    for key in path.parts:
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur


def _write_path(dst: Dict[str, Any], path: _Path, value: Any) -> None:
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
