from typing import Any, AsyncIterator, Dict, TYPE_CHECKING

from etl_core.strategies.base_strategy import ExecutionStrategy
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics

if TYPE_CHECKING:
    from etl_core.components.base_component import Component


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
