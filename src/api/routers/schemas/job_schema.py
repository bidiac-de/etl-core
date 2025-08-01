from fastapi import APIRouter
from src.job_execution.job import Job
from src.components.component_registry import component_registry
from src.api.helpers import inline_defs

router = APIRouter(
    prefix="/jobs",
    tags=["jobs"],
)


@router.get(
    "/schema",
    response_model=dict,
    summary="Get Job JSON schema (with dataclasses & enums inlined)",
    description=(
        "Returns the JSON Schema for the Job configuration payload, "
        "with Layout, MetaData, RuntimeState, etc. nested directly."
    ),
)
def get_job_schema() -> dict:
    # Generate full schema (with $defs for dataclasses/enums)
    schema = Job.model_json_schema()

    # Patch components list to link to each component‐type schema:
    schema["properties"]["components"]["items"] = {
        "oneOf": [
            {"$ref": f"/components/{comp_type}/schema"}
            for comp_type in component_registry.keys()
        ]
    }

    # Inline all $defs then drop $defs
    schema = inline_defs(schema)
    return schema
