from __future__ import annotations

from threading import RLock
from typing import Any, Dict, List, Tuple

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import ValidationError

from etl_core.api.helpers import _error_payload, _exc_meta, inline_defs
from etl_core.components.component_registry import (
    RegistryMode,
    component_meta,
    component_registry,
    get_registry_mode,
    public_component_types,
)
from etl_core.persistence.base_models.job_base import JobBase
from etl_core.security.dependencies import require_authorized_client

router = APIRouter(
    prefix="/configs",
    tags=["Configs"],
    dependencies=[Depends(require_authorized_client)],
)

# Simple, thread-safe in-process caches
# Keys include registry mode so switches don't leak
_JOB_SCHEMA_CACHE: Dict[str, Dict[str, Any]] = {}
_COMPONENT_SCHEMA_CACHE: Dict[Tuple[str, str], Dict[str, Any]] = {}
_COMPONENT_TYPES_CACHE: Dict[str, List[str]] = {}

_CACHE_LOCK = RLock()


def invalidate_schema_caches() -> None:
    """
    Clear all schema caches. Call after changing registry mode/registry.
    """
    with _CACHE_LOCK:
        _JOB_SCHEMA_CACHE.clear()
        _COMPONENT_SCHEMA_CACHE.clear()
        _COMPONENT_TYPES_CACHE.clear()


def _cached_job_schema() -> Dict[str, Any]:
    mode = get_registry_mode()
    mode_key = getattr(mode, "value", str(mode))
    with _CACHE_LOCK:
        hit = _JOB_SCHEMA_CACHE.get(mode_key)
        if hit is not None:
            return hit
    # Compute outside dict mutation to minimize lock hold time
    computed = inline_defs(JobBase.model_json_schema())
    with _CACHE_LOCK:
        _JOB_SCHEMA_CACHE[mode_key] = computed
    return computed


def _cached_component_schema(comp_type: str) -> Dict[str, Any]:
    mode = get_registry_mode()
    mode_key = getattr(mode, "value", str(mode))
    cache_key = (comp_type, mode_key)

    with _CACHE_LOCK:
        hit = _COMPONENT_SCHEMA_CACHE.get(cache_key)
        if hit is not None:
            return hit

    # Visibility/existence checks have to run
    meta = component_meta(comp_type)
    if mode == RegistryMode.PRODUCTION and (meta is None or meta.hidden):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=_error_payload(
                "SCHEMA_COMPONENT_HIDDEN",
                f"Unknown component {comp_type!r}",
                comp_type=comp_type,
                registry_mode=mode_key,
            ),
        )

    cls = component_registry.get(comp_type)
    if cls is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=_error_payload(
                "SCHEMA_COMPONENT_UNKNOWN",
                f"Unknown component {comp_type!r}",
                comp_type=comp_type,
                registry_mode=mode_key,
            ),
        )

    computed = inline_defs(cls.model_json_schema())
    with _CACHE_LOCK:
        _COMPONENT_SCHEMA_CACHE[cache_key] = computed
    return computed


def _cached_component_types() -> List[str]:
    """
    Cache component type listing per registry mode, so /configs/component_types
    is served from memory until registry mode/contents change.
    """
    mode = get_registry_mode()
    mode_key = getattr(mode, "value", str(mode))
    with _CACHE_LOCK:
        hit = _COMPONENT_TYPES_CACHE.get(mode_key)
        if hit is not None:
            return list(hit)

    # computing outside of lock to reduce contention
    types = public_component_types()
    with _CACHE_LOCK:
        _COMPONENT_TYPES_CACHE[mode_key] = list(types)
        return list(types)


# Routes
@router.get(
    "/job",
    response_model=dict,
    summary="Get Job JSON schema (with dataclasses & enums inlined)",
)
def get_job_schema() -> Dict[str, Any]:
    try:
        return _cached_job_schema()
    except ValidationError as exc:
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
        return _cached_component_types()
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
        return _cached_component_schema(comp_type)
    except HTTPException:
        raise
    except ValidationError as exc:
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
