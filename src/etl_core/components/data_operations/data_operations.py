from pydantic import ConfigDict
from abc import ABC
from etl_core.components.base_component import Component


class DataOperationsComponent(Component, ABC):
    """
    Concrete data operations component, e.g. filter, map, transform.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )
