from __future__ import annotations

from typing import List, Dict, Any

from fastapi import APIRouter, HTTPException, status
from pydantic import ValidationError

from src.persistance.base_models.job_base import JobBase
from src.api.helpers import inline_defs, _error_payload, _exc_meta
from src.components.component_registry import (
    component_registry,
    public_component_types,
    component_meta,
    get_registry_mode,
    RegistryMode,
)

router = APIRouter(prefix="/configs", tags=["configs"])


@router.get(
    "/job",
    response_model=dict,
    summary="Get Job JSON schema (with dataclasses & enums inlined)",
)
def get_job_schema() -> Dict[str, Any]:
    try:
        schema = JobBase.model_json_schema()
        return inline_defs(schema)
    except ValidationError as exc:
        # Extremely rare here, but keep parity with /jobs
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=_error_payload(
                "SCHEMA_JOB_INVALID",
                "JobBase schema validation failed.",
                **_exc_meta(exc),
            ),
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_error_payload(
                "SCHEMA_JOB_GENERATION_FAILED",
                "Failed to generate Job JSON Schema.",
                **_exc_meta(exc),
            ),
        ) from exc


@router.get(
    "/component_types",
    response_model=List[str],
    summary="List all available concrete component types",
)
def list_component_types() -> List[str]:
    try:
        # Only show non-hidden components to the GUI
        return public_component_types()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_error_payload(
                "SCHEMA_COMPONENT_TYPES_FAILED",
                "Failed to list component types.",
                **_exc_meta(exc),
            ),
        ) from exc


@router.get(
    "/{comp_type}",
    response_model=dict,
    summary="Get JSON Schema for a specific component (inlined)",
)
def get_component_schema(comp_type: str) -> Dict[str, Any]:
    try:
        mode = get_registry_mode()
        meta = component_meta(comp_type)

        # In production, hidden or unknown components must 404
        if mode == RegistryMode.PRODUCTION and (meta is None or meta.hidden):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=_error_payload(
                    "SCHEMA_COMPONENT_HIDDEN",
                    f"Unknown component {comp_type!r}",
                    comp_type=comp_type,
                    registry_mode=mode.value if hasattr(mode, "value") else str(mode),
                ),
            )

        cls = component_registry.get(comp_type)
        if cls is None:
            # Outside production we still emit a 404, but with a different code
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=_error_payload(
                    "SCHEMA_COMPONENT_UNKNOWN",
                    f"Unknown component {comp_type!r}",
                    comp_type=comp_type,
                    registry_mode=mode.value if hasattr(mode, "value") else str(mode),
                ),
            )

        return inline_defs(cls.model_json_schema())

    except HTTPException:
        # Re-raise cleanly to preserve our intended payloads/status
        raise
    except ValidationError as exc:
        # If the components Pydantic model fails to produce a schema
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=_error_payload(
                "SCHEMA_COMPONENT_INVALID",
                f"Component {comp_type!r} schema validation failed.",
                comp_type=comp_type,
                **_exc_meta(exc),
            ),
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_error_payload(
                "SCHEMA_COMPONENT_GENERATION_FAILED",
                f"Failed to generate JSON Schema for component {comp_type!r}.",
                comp_type=comp_type,
                **_exc_meta(exc),
            ),
        ) from exc
