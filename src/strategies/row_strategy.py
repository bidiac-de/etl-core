from typing import Any, AsyncIterator, TYPE_CHECKING

from src.strategies.base_strategy import ExecutionStrategy
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.components.envelopes import Out

if TYPE_CHECKING:
    from src.components.base_component import Component


class RowExecutionStrategy(ExecutionStrategy):
    """
    True streaming row-by-row mode:
    Delegates to component.stream_rows(...) which MUST be an async-iterable.
    """

    async def execute(
        self, component: "Component", payload: Any, metrics: ComponentMetrics
    ) -> AsyncIterator[Out]:
        async for item in component.process_row(payload, metrics):
            if not isinstance(item, Out):
                raise TypeError(
                    f"{component.name}.process_row must yield Out(port, payload)"
                )
            yield item
