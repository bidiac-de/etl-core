from typing import Any, Iterable, List, Set
from collections import Counter

from pydantic import ConfigDict, Field, NonNegativeInt, field_validator, model_validator

from etl_core.persistence.base_models.job_base import JobBase
from etl_core.persistence.base_models.dataclasses_base import MetaDataBase
from etl_core.components.base_component import Component
from etl_core.components.component_registry import component_registry
from etl_core.components.wiring.ports import EdgeRef
from etl_core.utils.common_helpers import assert_unique
from etl_core.job_execution.runtimejob import RuntimeJob  # <- deep validation import


def _known_component_types() -> Set[str]:
    return set(component_registry.keys())


def _assert_known_types(components: Iterable[Component], valid: Set[str]) -> None:
    for comp in components:
        if comp.comp_type not in valid:
            raise ValueError(f"Unknown component type: {comp.comp_type!r}")


def _assert_unique_names(components: Iterable[Component]) -> None:
    counts = Counter(c.name for c in components)
    dupes = [name for name, amt in counts.items() if amt > 1]
    if dupes:
        raise ValueError(f"Duplicate component names: {sorted(dupes)}")


def _extract_target_name(ref: object) -> str:
    if isinstance(ref, EdgeRef):
        return ref.to
    if isinstance(ref, str):
        return ref
    raise TypeError("route entries must be EdgeRef or str, " f"got {type(ref)}")


def _assert_routes_point_to_existing(components: Iterable[Component]) -> None:
    names = {c.name for c in components}
    for comp in components:
        routes = getattr(comp, "routes", {}) or {}
        for targets in routes.values():
            for target in targets:
                target_name = _extract_target_name(target)
                if target_name not in names:
                    raise ValueError(
                        f"{comp.name}: routes reference unknown"
                        f" component {target_name!r}"
                    )


class JobConfig(JobBase):
    """
    Config model used by endpoints/tests to:
      - inflate component dicts into concrete Component subclasses via the registry
      - validate top-level job fields and graph-level sanity
      - **NEW**: perform deep runtime-equivalent validation
        (wiring, port contracts, schema presence) by instantiating a RuntimeJob.
    """

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

            comp_cls = component_registry.get(comp_type)
            if comp_cls is None:
                raise ValueError(f"Unknown component type: {comp_type!r}")

            # let the concrete class validate itself
            inflated.append(comp_cls(**item))
        return inflated

    @model_validator(mode="after")
    def _validate_components_shallow(self) -> "JobConfig":
        """
        Keep this layer shallow to provide early, focused messages.
        """
        valid_types = _known_component_types()
        _assert_known_types(self.components, valid_types)
        assert_unique([c.name for c in self.components], context="component names")
        _assert_routes_point_to_existing(self.components)
        return self

    @model_validator(mode="after")
    def _validate_runtime_equivalence(self) -> "JobConfig":
        """
        Deep validation pass:
        - clone components to avoid mutating the config
        - instantiate a RuntimeJob to reuse runtime wiring + port/schema checks
        If the runtime would fail, fail here with the same messages.
        """
        cloned_components: List[Component] = [
            c.model_copy(deep=True) for c in self.components
        ]

        _ = RuntimeJob(
            name=self.name,
            num_of_retries=self.num_of_retries,
            file_logging=self.file_logging,
            strategy_type=self.strategy_type,
            components=cloned_components,
            metadata_={},  # runtime doesn't need persisted metadata for validation
        )
        return self

    @field_validator("name", "strategy_type", mode="before")
    @classmethod
    def _validate_non_empty_string(cls, value: str) -> str:
        """Validate that the field is a non-empty string."""
        if not isinstance(value, str) or not value.strip():
            raise ValueError("Value must be a non-empty string.")
        return value.strip()

    @field_validator("num_of_retries", mode="before")
    @classmethod
    def _validate_num_of_retries(cls, value: int) -> NonNegativeInt:
        """Validate that the number of retries is a non-negative integer."""
        if not isinstance(value, int) or value < 0:
            raise ValueError("Number of retries must be a non-negative integer.")
        return NonNegativeInt(value)

    @field_validator("file_logging", mode="before")
    @classmethod
    def _validate_file_logging(cls, value: bool) -> bool:
        """Validate that file_logging is a boolean."""
        if not isinstance(value, bool):
            raise ValueError("File logging must be a boolean value.")
        return value
