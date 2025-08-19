from typing import Type
from etl_core.components.base_component import Component

component_registry: dict[str, Type[Component]] = {}


def register_component(type_name: str):
    """
    Decorator to register a Component subclass under `type_name`.
    """

    def decorator(cls: Type[Component]) -> Type[Component]:
        component_registry[type_name] = cls
        return cls

    return decorator
