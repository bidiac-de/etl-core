from abc import ABC
from typing import Optional

from pydantic import ConfigDict, Field

from etl_core.components.file_components.file_component import FileComponent


class Excel(FileComponent, ABC):
    """Abstract base class for Excel file components."""

    ICON = "ti ti-file-type-xls"
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )

    sheet_name: Optional[str] = Field(
        default="Sheet1", description="Default Excel sheet name"
    )
