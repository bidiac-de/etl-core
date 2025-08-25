from typing import Any, AsyncIterator, TYPE_CHECKING

from etl_core.strategies.base_strategy import ExecutionStrategy
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.components.envelopes import Out

if TYPE_CHECKING:
    from etl_core.components.base_component import Component


class RowExecutionStrategy(ExecutionStrategy):
    """
    True streaming row-by-row mode:
    Delegates to component.process_rows(...) which MUST be an async-iterable.
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
