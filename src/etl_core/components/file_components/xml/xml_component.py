from abc import ABC
from pydantic import Field, ConfigDict
from etl_core.components.file_components.file_component import FileComponent


class XML(FileComponent, ABC):
    """Abstract base class for XML file components."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )

    root_tag: str = Field(
        default="rows", description="Root element for bulk/bigdata writes"
    )
    record_tag: str = Field(
        default="row", description="Element name for a single record"
    )
