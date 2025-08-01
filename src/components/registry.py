from typing import Type
from src.components.base_components import Component

component_registry: dict[str, Type[Component]] = {}


def register_component(type_name: str):
    """
    Decorator to register a Component subclass under `type_name`.
    """

    def decorator(cls: Type[Component]) -> Type[Component]:
        component_registry[type_name] = cls
        return cls

    return decorator
