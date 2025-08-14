from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Iterable, Type

from src.components.base_component import Component


component_registry: Dict[str, Type[Component]] = {}


@dataclass(frozen=True)
class ComponentMeta:
    hidden: bool = False
    tags: tuple[str, ...] = ()


# name -> meta
_component_meta: Dict[str, ComponentMeta] = {}


class RegistryMode(str, Enum):
    PRODUCTION = "production"
    TEST = "test"
    ALL = "all"  # helpful override, could be used if tests differ


# Default to PRODUCTION unless set at startup
_CURRENT_MODE: RegistryMode = RegistryMode.PRODUCTION


def set_registry_mode(mode: RegistryMode) -> None:
    """
    Set the global visibility mode. Call this once at application startup.
    """
    global _CURRENT_MODE
    _CURRENT_MODE = mode


def get_registry_mode() -> RegistryMode:
    return _CURRENT_MODE


def register_component(
    type_name: str,
    *,
    hidden: bool = False,
    tags: Iterable[str] = (),
):
    """
    Decorator to register a Component subclass under `type_name`.

    Usage:
        @register_component("mytype", hidden=True)
        class MyComp(Component): ...
    """
    tag_tuple = tuple(tags)

    def decorator(cls: Type[Component]) -> Type[Component]:
        component_registry[type_name] = cls
        _component_meta[type_name] = ComponentMeta(hidden=hidden, tags=tag_tuple)
        return cls

    return decorator


def get_component_class(type_name: str) -> Type[Component] | None:
    return component_registry.get(type_name)


def _is_visible(type_name: str, mode: RegistryMode) -> bool:
    meta = _component_meta.get(type_name, ComponentMeta())
    if mode == RegistryMode.PRODUCTION:
        return not meta.hidden
    # TEST and ALL expose everything
    return True


def public_component_types(mode: RegistryMode | None = None) -> list[str]:
    """
    Return component type names visible in `mode` (or the globally set mode).
    """
    effective = mode or _CURRENT_MODE
    return [name for name in component_registry if _is_visible(name, effective)]


def component_meta(type_name: str) -> ComponentMeta | None:
    return _component_meta.get(type_name)
