from __future__ import annotations

from typing import Any, AsyncGenerator, Dict, List, Tuple, Union, Protocol

import pandas as pd
import dask.dataframe as dd
from etl_core.receivers.base_receiver import Receiver
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa: E501
    DataOperationsMetrics,
)

AggDict = Dict[str, Any]


class GroupedLike(Protocol):
    """Protocol for objects returned by pandas/dask groupby."""

    def agg(self, agg_map: Dict[str, List[str]]) -> Any: ...
    def size(self) -> Any: ...
    def reset_index(self, *a: Any, **kw: Any) -> Any: ...


def _to_pd(rows: List[Dict[str, Any]], sep: str = ".") -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.json_normalize(rows, sep=sep)


def _build_agg_plan(
    aggs: List[AggDict],
) -> Tuple[Dict[str, List[str]], List[Tuple[str, str, str]]]:
    """Build the aggregation plan, accepting either strings or Enum values for 'op'."""
    allowed = {"count", "sum", "min", "max", "mean", "median", "std", "nunique"}
    agg_map: Dict[str, List[str]] = {}
    order: List[Tuple[str, str, str]] = []

    for a in aggs:
        src = a["src"]
        op_raw = a["op"]
        # op can be a string or an Enum
        op_val = getattr(op_raw, "value", op_raw)
        func = str(op_val).lower()
        dest = a["dest"]

        if func not in allowed:
            raise ValueError(f"Unsupported aggregation '{func}'")

        if src != "*":
            funcs = agg_map.setdefault(src, [])
            if func not in funcs:
                funcs.append(func)

        order.append((src, func, dest))

    return agg_map, order


def _ensure_keys(
    df: Union[pd.DataFrame, dd.DataFrame], group_by: List[str]
) -> Tuple[Union[pd.DataFrame, dd.DataFrame], List[str]]:
    if group_by:
        return df, list(group_by)
    df = df.assign(_grp_const_=1)
    return df, ["_grp_const_"]


def _strip_const_key(df: Union[pd.DataFrame, dd.DataFrame]) -> Any:
    if "_grp_const_" in df.columns:
        return df.drop(columns=["_grp_const_"])
    return df


def _split_nunique(
    order: List[Tuple[str, str, str]],
    agg_map: Dict[str, List[str]],
) -> Tuple[List[Tuple[str, str, str]], Dict[str, List[str]]]:
    """Return (nunique_specs, agg_map_without_nunique)."""
    nunique_specs = [(s, f, d) for (s, f, d) in order if f == "nunique" and s != "*"]
    agg_map_no_nunique: Dict[str, List[str]] = {}
    for col, funcs in agg_map.items():
        kept = [f for f in funcs if f != "nunique"]
        if kept:
            agg_map_no_nunique[col] = kept
    return nunique_specs, agg_map_no_nunique


def _agg_base(grouped: Any, agg_map_no_nunique: Dict[str, List[str]]) -> Any:
    """Run base aggregation (without nunique), flatten columns, reset index."""
    out = (
        grouped.agg(agg_map_no_nunique)
        if agg_map_no_nunique
        else grouped.size().to_frame("_dummy_")
    )
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = [f"{c[0]}__{c[1]}" for c in out.columns]
    return out.reset_index()


def _attach_size(
    grouped: Any, out: Any, keys: List[str], order: List[Tuple[str, str, str]]
) -> Any:
    """Attach count(*) if requested via '*'."""
    if not any(src == "*" for src, _f, _d in order):
        return out
    size_series = grouped.size()
    try:
        size = size_series.reset_index(name="_rows_")
    except TypeError:
        size = size_series.to_frame("_rows_").reset_index()
    return out.merge(size, on=keys, how="left")


def _merge_nunique(
    grouped: Any,
    out: Any,
    keys: List[str],
    nunique_specs: List[Tuple[str, str, str]],
) -> Any:
    """Compute nunique per required src and merge into out."""
    for src, _func, _dest in nunique_specs:
        series = grouped[src].nunique()
        try:
            nunq = series.rename(f"{src}__nunique").reset_index()
        except TypeError:
            nunq = series.to_frame(f"{src}__nunique").reset_index()
        out = out.merge(nunq, on=keys, how="left")
    return out


def _finalize_columns(
    out: Any,
    keys: List[str],
    order: List[Tuple[str, str, str]],
) -> Any:
    """Order and rename columns according to the declared order."""
    cols: List[str] = []
    rename: Dict[str, str] = {}
    for src, func, dest in order:
        if src == "*":
            cols.append("_rows_")
            rename["_rows_"] = dest
        else:
            col = f"{src}__{func}"
            cols.append(col)
            rename[col] = dest
    existing = [c for c in keys + cols if c in out.columns]
    return out[existing].rename(columns=rename)


def _apply_grouped(
    grouped: "GroupedLike",
    keys: List[str],
    agg_map: Dict[str, List[str]],
    order: List[Tuple[str, str, str]],
) -> Any:
    """
    Apply groupby aggregations with special handling for `nunique` and count(*)
    in a backend-agnostic way (pandas & dask).
    """
    nunique_specs, agg_map_no_nunique = _split_nunique(order, agg_map)
    out = _agg_base(grouped, agg_map_no_nunique)
    out = _attach_size(grouped, out, keys, order)
    out = _merge_nunique(grouped, out, keys, nunique_specs)
    return _finalize_columns(out, keys, order)


class AggregationReceiver(Receiver):
    """Group-by aggregations for dict rows, pandas, and dask frames."""

    async def process_rows(
        self,
        *,
        rows: List[Dict[str, Any]],
        group_by: List[str],
        aggregations: List[AggDict],
        metrics: DataOperationsMetrics,
    ) -> AsyncGenerator[Tuple[str, Dict[str, Any]], None]:
        df = _to_pd(rows)
        metrics.lines_received += int(df.shape[0])
        if df.empty:
            return

        df, keys = _ensure_keys(df, group_by)
        agg_map, order = _build_agg_plan(aggregations)
        out_df = _apply_grouped(df.groupby(keys, dropna=False), keys, agg_map, order)
        out_df = _strip_const_key(out_df)

        n = int(out_df.shape[0])
        metrics.lines_processed += n
        metrics.lines_forwarded += n

        for rec in out_df.to_dict(orient="records"):
            yield "out", rec

    async def process_bulk(
        self,
        *,
        dataframe: pd.DataFrame,
        group_by: List[str],
        aggregations: List[AggDict],
        metrics: DataOperationsMetrics,
    ) -> AsyncGenerator[Tuple[str, pd.DataFrame], None]:
        metrics.lines_received += int(dataframe.shape[0])
        if dataframe.empty:
            yield "out", dataframe.head(0)
            return

        df, keys = _ensure_keys(dataframe, group_by)
        agg_map, order = _build_agg_plan(aggregations)
        out_df = _apply_grouped(df.groupby(keys, dropna=False), keys, agg_map, order)
        out_df = _strip_const_key(out_df)

        n = int(out_df.shape[0])
        metrics.lines_processed += n
        metrics.lines_forwarded += n
        yield "out", out_df.reset_index(drop=True)

    async def process_bigdata(
        self,
        *,
        ddf: dd.DataFrame,
        group_by: List[str],
        aggregations: List[AggDict],
        metrics: DataOperationsMetrics,
    ) -> AsyncGenerator[Tuple[str, dd.DataFrame], None]:
        metrics.lines_received += int(ddf.map_partitions(len).sum().compute())
        ddf, keys = _ensure_keys(ddf, group_by)
        agg_map, order = _build_agg_plan(aggregations)

        out_ddf = _apply_grouped(ddf.groupby(keys, dropna=False), keys, agg_map, order)
        out_ddf = _strip_const_key(out_ddf)

        n = int(out_ddf.map_partitions(len).sum().compute())
        metrics.lines_processed += n
        metrics.lines_forwarded += n
        yield "out", out_ddf
