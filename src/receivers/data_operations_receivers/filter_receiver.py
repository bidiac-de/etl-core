from __future__ import annotations
from typing import Any, AsyncIterator, Dict
import asyncio
import pandas as pd
import dask.dataframe as dd

from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.receivers.data_operations_receivers.data_operations_receiver import DataOperationsReceiver
from src.components.data_operations.comparison_rule import ComparisonRule
from src.receivers.data_operations_receivers.filter_helper import eval_rule_on_row, eval_rule_on_frame


class FilterReceiver(DataOperationsReceiver):
    """Applies filter rules to row/chunk streams or Dask DataFrames."""

    async def process_row(
            self,
            rows: AsyncIterator[Dict[str, Any]],
            *,
            metrics: ComponentMetrics,
            rule: ComparisonRule,
            **_: Any,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Filter incoming rows one by one."""
        async for row in rows:
            if eval_rule_on_row(row, rule):
                yield row

    async def process_bulk(
            self,
            frames: AsyncIterator[pd.DataFrame],
            *,
            metrics: ComponentMetrics,
            rule: ComparisonRule,
            **_: Any,
    ) -> AsyncIterator[pd.DataFrame]:
        """Filter incoming DataFrames in bulk."""
        async for pdf in frames:
            mask = await asyncio.to_thread(eval_rule_on_frame, pdf, rule)
            out = pdf[mask]
            if not out.empty:
                yield out

    async def process_bigdata(
            self,
            ddf: dd.DataFrame,
            *,
            metrics: ComponentMetrics,
            rule: ComparisonRule,
            **_: Any,
    ) -> dd.DataFrame:
        """Filter a large Dask DataFrame at the partition level."""

        def _apply(pdf: pd.DataFrame) -> pd.DataFrame:
            m = eval_rule_on_frame(pdf, rule)
            return pdf[m]

        return ddf.map_partitions(_apply, meta=dd.utils.make_meta(ddf))