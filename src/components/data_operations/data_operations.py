from pydantic import ConfigDict
from abc import ABC
from src.components.base_component import Component


class DataOperationsComponent(Component, ABC):
    """Concrete data operations component, e.g. filter, map, transform."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )
