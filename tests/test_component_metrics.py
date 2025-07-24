from src.metrics.component_metrics import ComponentMetrics
from datetime import datetime, timedelta

def test_component_metrics_default_lines():
    metrics = ComponentMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(seconds=0),
        error_count=0
    )

    assert metrics.lines_received == 0
    assert metrics.lines_forwarded == 0