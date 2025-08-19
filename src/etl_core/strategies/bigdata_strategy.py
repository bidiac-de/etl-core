from typing import Any, AsyncIterator, TYPE_CHECKING

from etl_core.strategies.base_strategy import ExecutionStrategy
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics

import dask.dataframe as dd

if TYPE_CHECKING:
    from etl_core.components.base_component import Component

class BigDataExecutionStrategy(ExecutionStrategy):
    """
    Streaming bigdata mode:
    Calls component.receiver.process_bigdata once and yields the Dask DataFrame.
    """

    async def execute(
        self,
        component: "Component",
        payload: Any,
        metrics: ComponentMetrics,
    ) -> AsyncIterator[dd.DataFrame]:

        async for ddf in component.process_bigdata(payload, metrics):
            yield ddf
