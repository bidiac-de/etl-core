from __future__ import annotations
from typing import Any, AsyncIterator, Dict
import pandas as pd
import dask.dataframe as dd
from pydantic import Field, ConfigDict, model_validator

from src.components.base_component import Component
from src.components.data_operations.comparison_rule import ComparisonRule
from src.receivers.data_operations_receivers.filter_receiver import FilterReceiver
from src.metrics.component_metrics.component_metrics import ComponentMetrics


class FilterComponent(Component):
    """Filter component: forwards data streams to the FilterReceiver."""

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

    rule: ComparisonRule = Field(...)

    @model_validator(mode="after")
    def _build_objects(self) -> "Filter":
        """Instantiate the receiver – no paths, no persistent state."""
        self._receiver = FilterReceiver()
        return self


    async def process_row(
            self,
            rows: AsyncIterator[Dict[str, Any]],
            *,
            metrics: ComponentMetrics,
            **kwargs: Any
    ) -> AsyncIterator[Dict[str, Any]]:
        """Process a stream of rows and apply the filter rule."""
        async for out in self.receiver.process_row(rows, metrics=metrics, rule=self.rule, **kwargs):
            yield out


    async def process_bulk(
            self,
            frames: AsyncIterator[pd.DataFrame],
            *,
            metrics: ComponentMetrics,
            **kwargs: Any
    ) -> AsyncIterator[pd.DataFrame]:
        """Process a stream of pandas DataFrames and apply the filter rule."""
        async for out in self.receiver.process_bulk(frames, metrics=metrics, rule=self.rule, **kwargs):
            yield out

    async def process_bigdata(
            self,
            ddf: dd.DataFrame,
            *,
            metrics: ComponentMetrics,
            **kwargs: Any
    ) -> dd.DataFrame:
        """Process a large Dask DataFrame and apply the filter rule."""
        return await self.receiver.process_bigdata(ddf, metrics=metrics, rule=self.rule, **kwargs)