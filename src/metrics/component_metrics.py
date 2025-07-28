from pydantic import Field

from src.metrics.base_metrics import Metrics


class ComponentMetrics(Metrics):
    """
    Metrics for stub Components
    """

    lines_received: int = Field(default=0)
    lines_forwarded: int = Field(default=0)
