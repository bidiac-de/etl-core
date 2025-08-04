from typing import Any, AsyncIterator
import dask.dataframe as dd

from src.strategies.base_strategy import ExecutionStrategy
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.components.base_component import Component


class BigDataExecutionStrategy(ExecutionStrategy):
    """
    Streaming bigdata mode:
    Calls component.receiver.process_bigdata once and yields the Dask DataFrame.
    """

    async def execute(
        self,
        component: Component,
        payload: Any,
        metrics: ComponentMetrics,
    ) -> AsyncIterator[dd.DataFrame]:
        ddf: dd.DataFrame = await component.process_bigdata(payload, metrics)
        yield ddf
