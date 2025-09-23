import asyncio
from typing import Any, AsyncIterator

import pytest

from etl_core.components.envelopes import Out
from etl_core.components.strategy_type import StrategyType
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.strategies.row_strategy import RowExecutionStrategy
from etl_core.strategies.bulk_strategy import BulkExecutionStrategy
from etl_core.strategies.bigdata_strategy import BigDataExecutionStrategy
from etl_core.strategies.base_strategy import ExecutionStrategy
from etl_core.components.base_component import get_strategy


class DummyComponent:
    def __init__(self, name: str = "dummy") -> None:
        self.name = name

    async def process_row(self, payload: Any, metrics: ComponentMetrics) -> AsyncIterator[Out]:
        yield Out("out", payload)

    async def process_bulk(self, payload: Any, metrics: ComponentMetrics) -> AsyncIterator[Out]:
        yield Out("out", payload)

    async def process_bigdata(self, payload: Any, metrics: ComponentMetrics) -> AsyncIterator[Out]:
        yield Out("out", payload)


@pytest.mark.asyncio
async def test_row_strategy_success() -> None:
    strat = RowExecutionStrategy()
    comp = DummyComponent("rowcomp")
    metrics = ComponentMetrics()
    result = []
    async for out in strat.execute(comp, {"a": 1}, metrics):
        result.append(out)
    assert len(result) == 1
    assert isinstance(result[0], Out)
    assert result[0].port == "out"
    assert result[0].payload == {"a": 1}


@pytest.mark.asyncio
async def test_row_strategy_type_error_when_not_out() -> None:
    class BadComponent(DummyComponent):
        async def process_row(self, payload: Any, metrics: ComponentMetrics):  # type: ignore[override]
            yield payload  # not an Out

    strat = RowExecutionStrategy()
    comp = BadComponent("badrow")
    metrics = ComponentMetrics()
    with pytest.raises(TypeError):
        async for _ in strat.execute(comp, 123, metrics):
            pass


@pytest.mark.asyncio
async def test_bulk_strategy_success_and_type_check() -> None:
    strat = BulkExecutionStrategy()
    comp = DummyComponent("bulkcomp")
    metrics = ComponentMetrics()
    outs = [out async for out in strat.execute(comp, [1, 2, 3], metrics)]
    assert len(outs) == 1 and isinstance(outs[0], Out)

    class BadBulk(DummyComponent):
        async def process_bulk(self, payload: Any, metrics: ComponentMetrics):  # type: ignore[override]
            yield "not-an-out"

    with pytest.raises(TypeError):
        async for _ in strat.execute(BadBulk("badbulk"), [1], metrics):
            pass


@pytest.mark.asyncio
async def test_bigdata_strategy_success_and_type_check() -> None:
    strat = BigDataExecutionStrategy()
    comp = DummyComponent("bdcomp")
    metrics = ComponentMetrics()
    outs = [out async for out in strat.execute(comp, {"rows": 10}, metrics)]
    assert len(outs) == 1 and isinstance(outs[0], Out)

    class BadBD(DummyComponent):
        async def process_bigdata(self, payload: Any, metrics: ComponentMetrics):  # type: ignore[override]
            yield 42

    with pytest.raises(TypeError):
        async for _ in strat.execute(BadBD("badbd"), None, metrics):
            pass


def test_execution_strategy_is_abstract() -> None:
    class BadStrategy(ExecutionStrategy):
        pass

    with pytest.raises(TypeError):
        BadStrategy()  # type: ignore[abstract]


def test_get_strategy_factory() -> None:
    assert isinstance(get_strategy(StrategyType.ROW), RowExecutionStrategy)
    assert isinstance(get_strategy(StrategyType.BULK), BulkExecutionStrategy)
    assert isinstance(get_strategy(StrategyType.BIGDATA), BigDataExecutionStrategy)
    with pytest.raises(ValueError):
        get_strategy("unknown")  # type: ignore[arg-type]
