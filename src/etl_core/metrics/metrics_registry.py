from typing import Type
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics

metrics_registry: dict[str, Type[ComponentMetrics]] = {}


def register_metrics(type_name: str):
    """
    Decorator to register a Metrics subclass for the given component type
    """

    def decorator(cls: Type[ComponentMetrics]) -> Type[ComponentMetrics]:
        metrics_registry[type_name] = cls
        return cls

    return decorator


def get_metrics_class(comp_type: str) -> Type[ComponentMetrics]:
    """
    Look up the appropriate Metrics class for a component
    fall back to the general ComponentMetrics if none registered.
    """
    return metrics_registry.get(comp_type, ComponentMetrics)
