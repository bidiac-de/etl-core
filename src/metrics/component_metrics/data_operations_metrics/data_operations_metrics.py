from pydantic import Field
from src.metrics.component_metrics.component_metrics import ComponentMetrics


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
