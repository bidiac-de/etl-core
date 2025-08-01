from fastapi import APIRouter, HTTPException
from typing import List
from src.components.component_registry import component_registry
from src.api.helpers import inline_defs

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
