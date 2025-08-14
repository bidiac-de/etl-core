from fastapi import APIRouter, HTTPException
from typing import List
from src.persistance.base_models.job_base import JobBase
from src.api.helpers import inline_defs
from src.components.component_registry import (
    component_registry,
    public_component_types,
)

router = APIRouter(prefix="/configs", tags=["configs"])


@router.get(
    "/job",
    response_model=dict,
    summary="Get Job JSON schema (with dataclasses & enums inlined)",
)
def get_job_schema() -> dict:
    schema = JobBase.model_json_schema()
    return inline_defs(schema)


@router.get(
    "/component_types",
    response_model=List[str],
    summary="List all available concrete component types",
)
def list_component_types() -> List[str]:
    # Only show non-hidden components to the GUI
    return public_component_types()


@router.get(
    "/{comp_type}",
    response_model=dict,
    summary="Get JSON Schema for a specific component (inlined)",
)
def get_component_schema(comp_type: str) -> dict:
    cls = component_registry.get(comp_type)
    if cls is None:
        raise HTTPException(status_code=404, detail=f"Unknown component {comp_type!r}")
    return inline_defs(cls.model_json_schema())
