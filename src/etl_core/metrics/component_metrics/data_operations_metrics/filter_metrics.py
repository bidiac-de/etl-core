from pydantic import Field, field_validator
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.metrics.metrics_registry import register_metrics


@register_metrics("filter")
class FilterMetrics(ComponentMetrics):
    """
    Metrics for Filter components.
    """

    lines_dismissed: int = Field(default=0)

    @field_validator("lines_dismissed")
    def validate_lines_dismissed(cls, value: int) -> int:
        if value < 0:
            raise ValueError("lines_dismissed must be a non-negative integer")
        return value
