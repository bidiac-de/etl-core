from typing import Any, AsyncIterator, Dict, TYPE_CHECKING

from src.strategies.base_strategy import ExecutionStrategy
from src.metrics.component_metrics.component_metrics import ComponentMetrics

if TYPE_CHECKING:
    from src.components.base_component import Component


class RowExecutionStrategy(ExecutionStrategy):
    """
    Streaming row-by-row mode:
    Repeatedly calls component.receiver.process_row until exhaustion.
    """

    async def execute(
            self,
            component: "Component",
            payload: Any,
            metrics: ComponentMetrics,
    ) -> AsyncIterator[Dict[str, Any]]:
        while True:
            try:
                row = await component.process_row(payload, metrics)
                yield row
            except StopAsyncIteration:
                break