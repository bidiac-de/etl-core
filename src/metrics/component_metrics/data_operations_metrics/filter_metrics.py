from pydantic import Field
from src.metrics.component_metrics.component_metrics import ComponentMetrics
from src.metrics.metrics_registry import register_metrics


@register_metrics("filter")
class FilterMetrics(ComponentMetrics):
    """
    Metrics for Filter components.
    """

    lines_dismissed: int = Field(default=0)
