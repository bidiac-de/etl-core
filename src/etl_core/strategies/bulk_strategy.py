from typing import Any, AsyncIterator, TYPE_CHECKING

from etl_core.strategies.base_strategy import ExecutionStrategy
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.components.envelopes import Out

if TYPE_CHECKING:
    from etl_core.components.base_component import Component


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
