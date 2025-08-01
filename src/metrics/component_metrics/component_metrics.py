from pydantic import Field

from src.metrics.base_metrics import Metrics


class ComponentMetrics(Metrics):
    """
    Metrics for Components, to be collected during execution
    """

    _lines_received: int = 0
    _lines_forwarded: int = 0

    @property
    def lines_received(self) -> int:
        return self._lines_received

    @lines_received.setter
    def lines_received(self, value: int) -> None:
        if not isinstance(value, int) or value < 0:
            raise ValueError("lines_received must be a non-negative integer")
        self._lines_received = value

    @property
    def lines_forwarded(self) -> int:
        return self._lines_forwarded

    @lines_forwarded.setter
    def lines_forwarded(self, value: int) -> None:
        if not isinstance(value, int) or value < 0:
            raise ValueError("lines_forwarded must be a non-negative integer")
        self._lines_forwarded = value
