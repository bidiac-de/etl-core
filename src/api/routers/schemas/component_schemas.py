from fastapi import APIRouter, HTTPException
from typing import List
from src.components.component_registry import component_registry

router = APIRouter(prefix="/components", tags=["components"])


@router.get(
    "",
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
    "/{comp_type}/schema",
    response_model=dict,
    summary="Get JSON Schema for a specific component",
)
def get_component_schema(comp_type: str) -> dict:
    """
    Return the JSON Schema for the concrete Component subclass
    identified by `comp_type`, stripping out any fields
    marked exclude=True (e.g. next_components).
    """
    cls = component_registry.get(comp_type)
    if cls is None:
        raise HTTPException(status_code=404, detail=f"Unknown component {comp_type!r}")

    # Generate the schema, using default Pydantic behavior
    schema: dict = cls.model_json_schema()

    return schema
