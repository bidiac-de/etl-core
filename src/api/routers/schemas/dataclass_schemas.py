from fastapi import APIRouter
from src.components.dataclasses import Layout, MetaData

router = APIRouter(prefix="/dataclasses", tags=["dataclasses"])


@router.get(
    "/layout",
    response_model=dict,
    summary="Get Layout model JSON schema",
    description="Returns the JSON Schema for the Layout Pydantic model."
)
def get_layout_schema() -> dict:
    """
    Return the JSON Schema for the Layout model, without nested $defs.
    """
    return Layout.model_json_schema(
        ref_template="/dataclasses/layout"
    )


@router.get(
    "/metadata",
    response_model=dict,
    summary="Get MetaData model JSON schema",
    description="Returns the JSON Schema for the MetaData Pydantic model."
)
def get_metadata_schema() -> dict:
    """
    Return the JSON Schema for the MetaData model, without nested $defs.
    """
    return MetaData.model_json_schema(ref_template="/dataclasses/metadata")
