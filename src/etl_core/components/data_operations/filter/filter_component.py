from __future__ import annotations

from typing import Any, AsyncIterator, Dict

import pandas as pd
import dask.dataframe as dd
from pydantic import ConfigDict, Field, model_validator

from etl_core.components.base_component import Component
from etl_core.components.component_registry import register_component
from etl_core.components.envelopes import Out
from etl_core.components.wiring.ports import InPortSpec, OutPortSpec
from etl_core.components.data_operations.filter.comparison_rule import ComparisonRule
from etl_core.metrics.component_metrics.data_operations_metrics.filter_metrics import (
    FilterMetrics,
)
from etl_core.receivers.data_operations_receivers.filter.filter_receiver import (
    FilterReceiver,
)


@register_component("filter")
class FilterComponent(Component):
    """
    Two-port filter:
    - input:  'in'   (required)
    - output: 'pass' (required) -> rows that meet the rule
              'fail' (optional) -> rows that do not meet the rule
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

    INPUT_PORTS = (InPortSpec(name="in", required=True, fanin="many"),)
    OUTPUT_PORTS = (
        OutPortSpec(name="pass", required=True, fanout="many"),
        OutPortSpec(name="fail", required=False, fanout="many"),
    )

    rule: ComparisonRule = Field(..., description="Filter rule expression.")

    @model_validator(mode="after")
    def _build_objects(self) -> "FilterComponent":
        self._receiver = FilterReceiver()
        return self

    async def process_row(
        self,
        row: Dict[str, Any],
        metrics: FilterMetrics,
    ) -> AsyncIterator[Out]:
        """
        Route rows to 'pass' if they satisfy the rule, otherwise to 'fail'.
        """
        if self._receiver is None:
            raise RuntimeError("FilterReceiver not initialized in process_row")
        async for port, payload in self._receiver.process_row(
            row=row, rule=self.rule, metrics=metrics
        ):
            yield Out(port=port, payload=payload)

    async def process_bulk(
        self,
        dataframe: pd.DataFrame,
        metrics: FilterMetrics,
    ) -> AsyncIterator[Out]:
        """
        Route DataFrame partitions to 'pass'/'fail'. Empty frames are skipped.
        """
        if self._receiver is None:
            raise RuntimeError("FilterReceiver not initialized in process_bulk")
        async for port, payload in self._receiver.process_bulk(
            dataframe=dataframe, rule=self.rule, metrics=metrics
        ):
            yield Out(port=port, payload=payload)

    async def process_bigdata(
        self,
        ddf: dd.DataFrame,
        metrics: FilterMetrics,
    ) -> AsyncIterator[Out]:
        """
        Route Dask DataFrames to 'pass'/'fail' without materializing them.
        """
        if self._receiver is None:
            raise RuntimeError("FilterReceiver not initialized in process_bigdata")
        async for port, payload in self._receiver.process_bigdata(
            ddf=ddf, rule=self.rule, metrics=metrics
        ):
            yield Out(port=port, payload=payload)
