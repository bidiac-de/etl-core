from typing import Any, AsyncIterator, Dict, TYPE_CHECKING

from src.strategies.base_strategy import ExecutionStrategy
from src.metrics.component_metrics.component_metrics import ComponentMetrics

if TYPE_CHECKING:
    from src.components.base_component import Component


class RowExecutionStrategy(ExecutionStrategy):
    """
    True streaming row-by-row mode:
    Delegates to component.stream_rows(...) which MUST be an async-iterable.
    """

    async def execute(
        self,
        component: "Component",
        payload: Any,
        metrics: ComponentMetrics,
    ) -> AsyncIterator[Dict[str, Any]]:
        async for row in component.process_row(payload, metrics):
            yield row