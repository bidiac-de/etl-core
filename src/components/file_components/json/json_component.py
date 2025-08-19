from abc import ABC
from pydantic import ConfigDict

from src.components.file_components.file_component import FileComponent


class JSON(FileComponent, ABC):
    """Abstract JSON component, async + streaming (yield)."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )
