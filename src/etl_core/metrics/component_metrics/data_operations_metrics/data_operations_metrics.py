from pydantic import Field
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.metrics.metrics_registry import register_metrics


@register_metrics("schema_mapping", "aggregation")
class DataOperationsMetrics(ComponentMetrics):
    """
    A class that offers metrics for data processing components.
    """

    lines_processed: int = Field(
        default=0, ge=0, description="Number of lines processed"
    )

    def __repr__(self):
        base = super().__repr__()[:-1]
        return f"{base}, lines_processed={self.lines_processed})"
