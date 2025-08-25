from typing import Any, AsyncIterator, TYPE_CHECKING
from etl_core.components.envelopes import Out

from etl_core.strategies.base_strategy import ExecutionStrategy
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics

if TYPE_CHECKING:
    from etl_core.components.base_component import Component


class BigDataExecutionStrategy(ExecutionStrategy):
    """
    Streaming bigdata mode:
    Calls component.receiver.process_bigdata once and yields an envelope
    containing the dask dataframe.
    """

    async def execute(  # noqa: D401
        self,
        component: "Component",
        payload: Any,
        metrics: ComponentMetrics,
    ) -> AsyncIterator[Out]:
        async for item in component.process_bigdata(payload, metrics):
            if not isinstance(item, Out):
                raise TypeError(
                    f"{component.name}.process_bigdata must yield Out(port, payload)"
                )
            yield item
