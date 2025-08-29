from abc import ABC
from pydantic import ConfigDict
from etl_core.components.file_components.file_component import FileComponent

class XML(FileComponent, ABC):
    """Abstract base class for XML file components."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )

    root_tag: str = "rows"
    record_tag: str = "row"


