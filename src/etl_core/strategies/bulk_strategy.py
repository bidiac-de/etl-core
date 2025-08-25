from typing import Any, AsyncIterator, TYPE_CHECKING

from etl_core.strategies.base_strategy import ExecutionStrategy
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics

import pandas as pd

if TYPE_CHECKING:
    from etl_core.components.base_component import Component


class BulkExecutionStrategy(ExecutionStrategy):
    """
    Streaming bulk mode:
    Calls component.receiver.process_bulk once and yields the DataFrame.
    """

    async def execute(
        self,
        component: "Component",
        payload: Any,
        metrics: ComponentMetrics,
    ) -> AsyncIterator[pd.DataFrame]:
        async for df in component.process_bulk(payload, metrics):
            yield df
