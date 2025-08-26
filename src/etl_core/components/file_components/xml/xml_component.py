# xml_component.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, AsyncGenerator, List, Optional, Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator
import pandas as pd
import dask.dataframe as dd

from etl_core.receivers.files.xml.xml_helper import Schema


# Reuse Schema from helper (or import your central one)

class FileComponent(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")
    name: str = Field(default="xml")
    filepath: Path = Field(default_factory=Path)
    schema: Optional[Schema] = Field(default=None, description="Optional schema for validation")

class XML(FileComponent):
    """Abstract XML component, async + streaming (yield) – analogous to JSON."""
    root_tag: str = Field(default="rows")
    record_tag: str = Field(default="row")

    # Interface methods are provided by concrete Read/Write components

