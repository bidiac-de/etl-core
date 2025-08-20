from __future__ import annotations

from typing import Any, AsyncGenerator, Dict

import pandas as pd
import dask.dataframe as dd

from etl_core.receivers.base_receiver import Receiver
from etl_core.components.data_operations.filter.comparison_rule import ComparisonRule
from etl_core.receivers.data_operations_receivers.filter.filter_helper import (
    eval_rule_on_frame,
    eval_rule_on_row,
)
from etl_core.metrics.component_metrics.data_operations_metrics.filter_metrics import (
    FilterMetrics,
)


class FilterReceiver(Receiver):
    """
    Receiver that applies filter rules to all execution paths.
    All public process_* methods are yield-only (streaming). No payload returns.
    Metrics are forwarded unchanged (execution layer does the accounting).
    """

    async def process_row(
        self,
        row: Dict[str, Any],
        rule: ComparisonRule,
        metrics: FilterMetrics,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Yield each incoming row that matches the rule.
        """
        metrics.lines_received += 1
        if eval_rule_on_row(row, rule):
            metrics.lines_forwarded += 1
            yield row
        else:
            # no match, no yield
            metrics.lines_dismissed += 1

    async def process_bulk(
        self,
        dataframe: pd.DataFrame,
        rule: ComparisonRule,
        metrics: FilterMetrics,
    ) -> AsyncGenerator[pd.DataFrame, None]:
        mask = eval_rule_on_frame(dataframe, rule)
        total_received = int(len(dataframe))
        total_forwarded = int(mask.sum())

        metrics.lines_received += total_received
        metrics.lines_forwarded += total_forwarded
        metrics.lines_dismissed += max(0, total_received - total_forwarded)

        if total_forwarded:
            yield dataframe[mask].reset_index(drop=True)
        else:
            # yield an empty frame if nothing matches
            yield dataframe.iloc[0:0].copy()

    async def process_bigdata(
        self,
        ddf: dd.DataFrame,
        rule: ComparisonRule,
        metrics: FilterMetrics,
    ) -> AsyncGenerator[dd.DataFrame, None]:
        def _apply(pdf: pd.DataFrame) -> pd.DataFrame:
            mask = eval_rule_on_frame(pdf, rule)
            return pdf[mask]

        filtered = ddf.map_partitions(
            _apply,
            meta=getattr(dd, "utils").make_meta(ddf),
        )

        # Best-effort metrics (safe-guarded)
        try:
            metrics.lines_received += int(ddf.map_partitions(len).sum().compute())
            metrics.lines_forwarded += int(filtered.map_partitions(len).sum().compute())
            metrics.lines_dismissed += max(
                0, metrics.lines_received - metrics.lines_forwarded
            )
        except Exception:
            pass

        yield filtered
