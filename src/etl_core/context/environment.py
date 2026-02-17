from __future__ import annotations

from enum import Enum
from typing import List, Union


class Environment(str, Enum):
    """
    Default well-known environments.

    The enum is kept for backward compatibility. However, the system now
    accepts **any** non-empty uppercase string as a valid environment
    value (e.g. ``"STAGING"``, ``"UAT"``). Custom environment values are
    derived from persisted contexts — if a context is saved with
    ``environment="STAGING"`` that value becomes available in the Studio
    execution menu.

    Use :func:`normalize_environment` to coerce any input into the
    canonical string form and :data:`DEFAULT_ENVIRONMENTS` for the
    built-in list.
    """

    DEV = "DEV"
    TEST = "TEST"
    PROD = "PROD"


# Ordered list of the built-in environment values.
DEFAULT_ENVIRONMENTS: List[str] = [e.value for e in Environment]

# Human-friendly labels used by the Studio UI.
ENVIRONMENT_LABELS: dict[str, str] = {
    "DEV": "Development",
    "TEST": "Test",
    "PROD": "Production",
}

# FontAwesome icons shown in the Studio execution menu.
ENVIRONMENT_ICONS: dict[str, str] = {
    "DEV": "fa-solid fa-bug",
    "TEST": "fa-solid fa-flask-vial",
    "PROD": "fa-solid fa-shield",
}


def normalize_environment(value: Union[str, Environment]) -> str:
    """Normalise *value* to the canonical uppercase string form.

    Accepts an ``Environment`` enum member or any non-empty string.
    Raises ``ValueError`` for blank / whitespace-only input.
    """
    if isinstance(value, Environment):
        return value.value
    raw = str(value).strip().upper()
    if not raw:
        raise ValueError("Environment value must not be empty.")
    return raw


def is_default_environment(value: str) -> bool:
    """Return ``True`` if *value* matches one of the built-in environments."""
    return value.strip().upper() in DEFAULT_ENVIRONMENTS
