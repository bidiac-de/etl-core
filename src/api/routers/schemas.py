from fastapi import APIRouter, HTTPException
from src.persistance.base_models.job_base import JobBase
from src.components.component_registry import component_registry
from src.api.helpers import inline_defs
from typing import List

router = APIRouter(
    prefix="/schemas",
    tags=["schemas"],
)


@router.get(
    "/job",
    response_model=dict,
    summary="Get Job JSON schema (with dataclasses & enums inlined)",
    description=(
        "Returns the JSON Schema for the Job configuration payload, "
        "with Layout, MetaData, RuntimeState, etc. nested directly."
    ),
)
def get_job_schema() -> dict:
    # Generate full schema (with $defs for dataclasses/enums)
    schema = JobBase.model_json_schema()

    # Inline all $defs then drop $defs
    schema = inline_defs(schema)
    return schema


@router.get(
    "/component_types",
    response_model=List[str],
    summary="List all available concrete component types",
)
def list_component_types() -> List[str]:
    """
    Return the list of all component type names
    registered via the @register_component decorator.
    """
    return list(component_registry.keys())


@router.get(
    "/{comp_type}",
    response_model=dict,
    summary="Get JSON Schema for a specific component (inlined)",
    description=("Returns the JSON Schema for the named Component subclass"),
)
def get_component_schema(comp_type: str) -> dict:
    cls = component_registry.get(comp_type)
    if cls is None:
        raise HTTPException(status_code=404, detail=f"Unknown component {comp_type!r}")

    # Generate the schema, letting Pydantic build $defs
    schema: dict = cls.model_json_schema()

    # Inline all needed $defs
    schema = inline_defs(schema)

    return schema
