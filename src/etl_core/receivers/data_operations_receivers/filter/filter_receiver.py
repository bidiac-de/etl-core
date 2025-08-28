from __future__ import annotations

from typing import Any, AsyncGenerator, Dict, Tuple
from uuid import uuid4
from uuid import uuid4

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


def _apply_filter_partition(pdf: pd.DataFrame, rule: ComparisonRule) -> pd.DataFrame:
    mask = eval_rule_on_frame(pdf, rule)
    return pdf[mask]


def _apply_remainder_partition(pdf: pd.DataFrame, rule: ComparisonRule) -> pd.DataFrame:
    mask = eval_rule_on_frame(pdf, rule)
    return pdf[~mask]


class FilterReceiver(Receiver):
    """
    Split incoming data into two streams:
      - 'pass': rows that match the rule
      - 'fail': rows that do not match the rule
    All methods yield (port, payload) tuples. Metrics are updated consistently.
    """

    @staticmethod
    def _apply_filter(pdf: pd.DataFrame, rule: ComparisonRule) -> pd.DataFrame:
        """Apply filter rule to a pandas DataFrame partition."""
        mask = eval_rule_on_frame(pdf, rule)
        return pdf[mask]

    async def process_row(
        self,
        row: Dict[str, Any],
        rule: ComparisonRule,
        metrics: FilterMetrics,
    ) -> AsyncGenerator[Tuple[str, Dict[str, Any]], None]:
        metrics.lines_received += 1
        if eval_rule_on_row(row, rule):
            metrics.lines_forwarded += 1
            yield "pass", row
        else:
            metrics.lines_dismissed += 1
            yield "fail", row

    async def process_bulk(
        self,
        dataframe: pd.DataFrame,
        rule: ComparisonRule,
        metrics: FilterMetrics,
    ) -> AsyncGenerator[Tuple[str, pd.DataFrame], None]:
        mask = eval_rule_on_frame(dataframe, rule)

        total_received = int(len(dataframe))
        total_pass = int(mask.sum())
        total_fail = total_received - total_pass

        metrics.lines_received += total_received
        metrics.lines_forwarded += total_pass
        metrics.lines_dismissed += max(0, total_fail)

        if total_pass:
            yield "pass", dataframe[mask].reset_index(drop=True)
        if total_fail:
            yield "fail", dataframe[~mask].reset_index(drop=True)

    def _filter_partition(self, pdf: pd.DataFrame, rule: ComparisonRule) -> pd.DataFrame:
        """Apply filter rule to a pandas DataFrame partition."""
        return self._apply_filter(pdf, rule)

    async def process_bigdata(
        self,
        ddf: dd.DataFrame,
        rule: ComparisonRule,
        metrics: FilterMetrics,
    ) -> AsyncGenerator[dd.DataFrame, None]:
        # Use a partial function to bind the rule parameter to the instance method
        from functools import partial
        
        filtered = ddf.map_partitions(
            partial(self._filter_partition, rule=rule),
            meta=make_meta(ddf),
        )

        # Best-effort metrics (safe-guarded)
        try:
            total_received = int(ddf.map_partitions(len).sum().compute())
            total_pass = int(passed.map_partitions(len).sum().compute())
            total_fail = max(0, total_received - total_pass)

            metrics.lines_received += total_received
            metrics.lines_forwarded += total_pass
            metrics.lines_dismissed += total_fail
        except Exception:
            pass

        yield "pass", passed
        yield "fail", failed
