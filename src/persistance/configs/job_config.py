from typing import List, Any
from pydantic import Field, ConfigDict, model_validator
from collections import Counter
from pydantic import NonNegativeInt, field_validator

from src.persistance.base_models.job_base import JobBase
from src.persistance.base_models.dataclasses_base import MetaDataBase
from src.components.component_registry import component_registry
from src.components.base_component import Component


class JobConfig(JobBase):
    components: List[Component] = Field(default_factory=list)
    metadata_: MetaDataBase = Field(default_factory=MetaDataBase)
    model_config = ConfigDict(validate_assignment=True, extra="ignore")

    @field_validator("components", mode="before")
    @classmethod
    def _inflate_components(cls, value: Any) -> List[Component]:
        if value is None:
            return []

        if not isinstance(value, list):
            raise TypeError("components must be a list of dicts or Component instances")

        inflated: List[Component] = []
        for item in value:
            if isinstance(item, Component):
                inflated.append(item)
                continue

            if not isinstance(item, dict):
                raise TypeError("each component must be a dict or a Component instance")

            comp_type = item.get("comp_type")
            if not isinstance(comp_type, str) or not comp_type.strip():
                raise ValueError("Component dict must include non-empty 'comp_type'")

            cls_ = component_registry.get(comp_type)
            if cls_ is None:
                raise ValueError(f"Unknown component type: {comp_type!r}")

            inflated.append(cls_(**item))  # lets the concrete class validate itself

        return inflated

    @model_validator(mode="after")
    def validate_components(self) -> "JobConfig":
        valid_types = set(component_registry.keys())
        for c in self.components:
            if c.comp_type not in valid_types:
                raise ValueError(f"Unknown component type: {c.comp_type!r}")
        counts = Counter(c.name for c in self.components)
        dupes = [n for n, k in counts.items() if k > 1]
        if dupes:
            raise ValueError(f"Duplicate component names: {sorted(dupes)}")
        names = {c.name for c in self.components}
        for c in self.components:
            for nxt in c.next:
                if nxt not in names:
                    raise ValueError(
                        f"Unknown next-component name {nxt!r} referenced by {c.name!r}"
                    )
        return self

    @field_validator("name", "strategy_type", mode="before")
    @classmethod
    def _validate_non_empty_string(cls, value: str) -> str:
        """
        Validate that the name, comp_type, and strategy_type are non-empty strings.
        """
        if not isinstance(value, str) or not value.strip():
            raise ValueError("Value must be a non-empty string.")
        return value.strip()

    @field_validator("num_of_retries", mode="before")
    @classmethod
    def _validate_num_of_retries(cls, value: int) -> NonNegativeInt:
        """
        Validate that the number of retries is a non-negative integer.
        """
        if not isinstance(value, int) or value < 0:
            raise ValueError("Number of retries must be a non-negative integer.")
        return NonNegativeInt(value)

    @field_validator("file_logging", mode="before")
    @classmethod
    def _validate_file_logging(cls, value: bool) -> bool:
        """
        Validate that file_logging is a boolean.
        """
        if not isinstance(value, bool):
            raise ValueError("File logging must be a boolean value.")
        return value
