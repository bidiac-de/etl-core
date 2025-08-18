from __future__ import annotations

from typing import Any, AsyncGenerator, Dict

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
            rows: Dict[str, Any],
            rule: ComparisonRule,
            metrics: FilterMetrics,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Yield each incoming row that matches the rule."""
        async for row in rows:
            metrics.lines_received += 1
            if eval_rule_on_row(row, rule):
                metrics.lines_forwarded += 1
                yield row

    async def process_bulk(
            self,
            frames: pd.DataFrame,
            rule: ComparisonRule,
            metrics: FilterMetrics,
    ) ->AsyncGenerator [pd.DataFrame]:
        """
        Consume incoming pandas DataFrame batches, filter them, and
        yield a single concatenated DataFrame if any rows matched.
        """
        parts: list[pd.DataFrame] = []
        total_received = 0
        total_forwarded = 0

        async for frame in frames:
            batch_size = int(len(frame))
            total_received += batch_size

            mask = eval_rule_on_frame(frame, rule)
            matched = int(mask.sum())
            total_forwarded += matched

            if matched:
                parts.append(frame[mask])

        metrics.lines_received = metrics.lines_received + total_received
        metrics.lines_forwarded = metrics.lines_forwarded + total_forwarded
        metrics.lines_dismissed = metrics.lines_dismissed + max(
            0, total_received - total_forwarded
)

        if parts:
            yield pd.concat(parts, ignore_index=True)


    async def process_bigdata(
            self,
            ddf: "dd.DataFrame",
            rule: ComparisonRule,
            metrics: FilterMetrics,
    ) -> AsyncGenerator["dd.DataFrame"]:
        """
        Build a lazily filtered Dask DataFrame and yield it exactly once.
        """
        try:
            total_received = int(ddf.map_partitions(len).sum().compute())
        except Exception:
            total_received = 0

        def _apply(partition_frame: pd.DataFrame) -> pd.DataFrame:
            mask = eval_rule_on_frame(partition_frame, rule)
            return partition_frame[mask]

        filtered = ddf.map_partitions(
            _apply,
            meta=getattr(dd, "utils").make_meta(ddf),
        )

        try:
            total_forwarded = int(filtered.map_partitions(len).sum().compute())
        except Exception:
            total_forwarded = 0

        metrics.lines_received = metrics.lines_received + total_received
        metrics.lines_forwarded = metrics.lines_forwarded + total_forwarded
        metrics.lines_dismissed = metrics.lines_dismissed + max(
            0, total_received - total_forwarded
        )
        yield filtered