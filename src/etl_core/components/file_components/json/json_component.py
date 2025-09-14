from abc import ABC
from pydantic import ConfigDict

from etl_core.components.file_components.file_component import FileComponent


class JSON(FileComponent, ABC):
    """Abstract JSON component, async + streaming (yield)."""

    ICON = "ti ti-json"

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )
