from typing import Any, AsyncIterator, TYPE_CHECKING
import dask.dataframe as dd

from src.strategies.base_strategy import ExecutionStrategy
from src.metrics.component_metrics.component_metrics import ComponentMetrics

if TYPE_CHECKING:
    from src.components.base_component import Component


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
