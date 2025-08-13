from __future__ import annotations

from typing import Any, AsyncIterator, Mapping

import pandas as pd

try:
    import dask.dataframe as dd
except Exception:
    dd = None
from src.receivers.base_receiver import Receiver
from src.components.data_operations.filter.comparison_rule import ComparisonRule
from src.receivers.data_operations_receivers.filter_helper import (
    eval_rule_on_frame,
    eval_rule_on_row,
)
from src.metrics.component_metrics.data_operations_metrics.filter_metrics import FilterMetrics


class FilterReceiver(Receiver):
    """
    Receiver that applies filter rules to all execution paths.
    All public process_* methods are yield-only (streaming). No payload returns.
    Metrics are forwarded unchanged (execution layer does the accounting).
    """

    async def process_row(
            self,
            rows: AsyncIterator[Mapping[str, Any]],
            *,
            metrics: FilterMetrics,
            rule: ComparisonRule,
    ) -> AsyncIterator[Mapping[str, Any]]:
        """Yield each incoming row that matches the rule."""
        _ = metrics
        async for row in rows:
            if eval_rule_on_row(row, rule):
                yield row

    async def process_bulk(
            self,
            frames: AsyncIterator[pd.DataFrame],
            *,
            metrics: FilterMetrics,
            rule: ComparisonRule,
    ) -> AsyncIterator[pd.DataFrame]:
        """
        Consume incoming pandas DataFrame batches, filter them, and
        yield a single concatenated DataFrame if any rows matched.
        """
        _ = metrics
        parts: list[pd.DataFrame] = []
        async for pdf in frames:
            mask = eval_rule_on_frame(pdf, rule)
            out = pdf[mask]
            if not out.empty:
                parts.append(out)

        if parts:
            yield pd.concat(parts, ignore_index=True)


    async def process_bigdata(
            self,
            ddf: "dd.DataFrame",
            *,
            metrics: FilterMetrics,
            rule: ComparisonRule,
    ) -> AsyncIterator["dd.DataFrame"]:
        """
        Build a lazily filtered Dask DataFrame and yield it exactly once.
        """
        _ = metrics

        def _apply(pdf: pd.DataFrame) -> pd.DataFrame:
            m = eval_rule_on_frame(pdf, rule)
            return pdf[m]

        filtered = ddf.map_partitions(
            _apply, meta=getattr(dd, "utils").make_meta(ddf)
        )
        yield filtered