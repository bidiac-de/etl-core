from typing import Any, AsyncIterator, TYPE_CHECKING
from src.components.envelopes import Out

from src.strategies.base_strategy import ExecutionStrategy
from src.metrics.component_metrics.component_metrics import ComponentMetrics

if TYPE_CHECKING:
    from src.components.base_component import Component


class BulkExecutionStrategy(ExecutionStrategy):
    """
    Streaming bulk mode:
    Calls component.receiver.process_bulk once and yields an envelope
    containing the pandas dataframe.
    """

    async def execute(  # noqa: D401
        self,
        component: "Component",
        payload: Any,
        metrics: ComponentMetrics,
    ) -> AsyncIterator[Out]:
        async for item in component.process_bulk(payload, metrics):
            if not isinstance(item, Out):
                raise TypeError(
                    f"{component.name}.process_bulk must yield Out(port, payload)"
                )
            yield item
