from typing import Any, AsyncIterator
import pandas as pd

from src.strategies.base_strategy import ExecutionStrategy
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.components.base_component import Component


class BulkExecutionStrategy(ExecutionStrategy):
    """
    Streaming bulk mode:
    Calls component.receiver.process_bulk once and yields the DataFrame.
    """

    async def execute(
        self,
        component: Component,
        payload: Any,
        metrics: ComponentMetrics,
    ) -> AsyncIterator[pd.DataFrame]:
        df: pd.DataFrame = await component.process_bulk(payload, metrics)
        yield df
