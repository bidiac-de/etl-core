from __future__ import annotations

from typing import AsyncIterator, Dict

import pandas as pd

try:
    import dask.dataframe as dd
except Exception:
    dd = None

from pydantic import ConfigDict, Field, model_validator

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

    @model_validator(mode="after")
    def _build_receiver(self) -> "FilterComponent":
        """Instantiate the receiver; no state is stored on it."""
        self._receiver = FilterReceiver()
        return self

    async def process_row(
        self,
        row: Dict[str, object],
        metrics: FilterMetrics,
    ) -> AsyncIterator[Dict[str, object]]:
        """Forward rows that satisfy the rule."""
        if self._receiver is None:
            raise RuntimeError("FilterReceiver not initialized in process_row")
        async for out in self._receiver.process_row(
            row, metrics=metrics, rule=self.rule
        ):
            yield out

    async def process_bulk(
        self,
        frames: AsyncIterator[pd.DataFrame],
        metrics: FilterMetrics,
    ) -> AsyncIterator[pd.DataFrame]:
        """
        Forward the single filtered DataFrame yielded by the receiver (if any).
        """
        if self._receiver is None:
            raise RuntimeError("FilterReceiver not initialized in process_bulk")
        async for df in self._receiver.process_bulk(
            frames, metrics=metrics, rule=self.rule
        ):
            yield df

    async def process_bigdata(
        self,
        ddf: "dd.DataFrame",
        metrics: FilterMetrics,
    ) -> AsyncIterator["dd.DataFrame"]:
        """Forward the lazily filtered Dask DataFrame yielded by the receiver."""
        if self._receiver is None:
            raise RuntimeError("FilterReceiver not initialized in process_bigdata")
        async for out_ddf in self._receiver.process_bigdata(
            ddf, metrics=metrics, rule=self.rule
        ):
            yield out_ddf
