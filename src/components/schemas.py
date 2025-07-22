from pydantic import Field
from typing import Union
from src.components.data_operations.filter import FilterComponentSchema

# filter is an example class, needs to be replaced with
# actual component classes when implemented
ComponentInfo = Union[
    FilterComponentSchema
    # ... more concrete component info to be added here
]
# Pydantic switches on type
ComponentInfoModel = Field(discriminator="type")
