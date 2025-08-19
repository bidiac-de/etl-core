from __future__ import annotations

from typing import AsyncIterator, Dict, Any

import pandas as pd
import dask.dataframe as dd

from pydantic import ConfigDict, Field

from src.components.data_operations.filter.comparison_rule import ComparisonRule
from src.components.base_component import Component
from src.components.component_registry import register_component
from src.metrics.component_metrics.data_operations_metrics.filter_metrics import (
    FilterMetrics,
)
from src.receivers.data_operations_receivers.filter_receiver import FilterReceiver


@register_component("filter")
class FilterComponent(Component):
    """
    Filter component that delegates to FilterReceiver.
    """

    def _build_objects(self) -> "FilterComponent":
        """
        Initialize receiver and return self.
        """
        self._receiver = FilterReceiver()
        return self

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

    rule: ComparisonRule = Field(..., description="Filter rule expression.")

    async def process_row(
        self,
        row: Dict[str, Any],
        metrics: FilterMetrics,
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Forward rows that satisfy the rule.
        """
        if self._receiver is None:
            raise RuntimeError("FilterReceiver not initialized in process_row")
        async for out in self._receiver.process_row(
            row, metrics=metrics, rule=self.rule
        ):
            yield out

    async def process_bulk(
        self,
        dataframe: pd.DataFrame,
        metrics: FilterMetrics,
    ) -> AsyncIterator[pd.DataFrame]:
        if self._receiver is None:
            raise RuntimeError("FilterReceiver not initialized in process_bulk")
        async for df in self._receiver.process_bulk(
            dataframe, metrics=metrics, rule=self.rule
        ):
            yield df

    async def process_bigdata(
        self,
        ddf: dd.DataFrame,
        metrics: FilterMetrics,
    ) -> AsyncIterator[dd.DataFrame]:
        if self._receiver is None:
            raise RuntimeError("FilterReceiver not initialized in process_bigdata")
        async for out in self._receiver.process_bigdata(
            ddf, metrics=metrics, rule=self.rule
        ):
            yield out
