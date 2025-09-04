from __future__ import annotations

from typing import Type

from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics

# Simple in-memory registry: comp_type -> Metrics subclass
metrics_registry: dict[str, Type[ComponentMetrics]] = {}


def register_metrics(*type_names: str):
    """
    Decorator to register a Metrics subclass for one or more component types.
    """

    def decorator(cls: Type[ComponentMetrics]) -> Type[ComponentMetrics]:
        for type_name in type_names:
            if not isinstance(type_name, str) or not type_name:
                raise ValueError("component type name must be a non-empty string")
            metrics_registry[type_name] = cls
        return cls

    return decorator


def get_metrics_class(comp_type: str) -> Type[ComponentMetrics]:
    """
    Look up the appropriate Metrics class for a component, falling back to the
    general ComponentMetrics if none is registered.
    """
    return metrics_registry.get(comp_type, ComponentMetrics)
