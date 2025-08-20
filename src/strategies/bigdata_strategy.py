from __future__ import annotations

import inspect
from typing import Any, AsyncIterator

import dask.dataframe as dd

from src.components.base_component import Component
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.strategies.base_strategy import ExecutionStrategy


class BigDataExecutionStrategy(ExecutionStrategy):
    """
    Executes a component in bigdata mode.
    Accepts component.process_bigdata implemented either as:
      - async generator yielding dd.DataFrame, or
      - async coroutine returning a dd.DataFrame.
    Yields Dask DataFrames uniformly.
    """

    async def execute(
        self,
        component: "Component",
        payload: Any,
        metrics: ComponentMetrics,
    ) -> AsyncIterator[dd.DataFrame]:
        result = component.process_bigdata(payload, metrics)

        if inspect.isasyncgen(result):
            async for ddf in result:
                yield ddf
            return

        if inspect.iscoroutine(result):
            ddf: dd.DataFrame = await result
            yield ddf
            return

        if result is not None:
            yield result  # type: ignore[misc]
