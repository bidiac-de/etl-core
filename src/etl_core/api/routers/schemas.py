from __future__ import annotations

from threading import RLock
from typing import Any, Dict, List, Tuple, Type, Optional

from fastapi import APIRouter, HTTPException, status
from pydantic import ValidationError

from etl_core.api.helpers import _error_payload, _exc_meta, inline_defs
from etl_core.components.component_registry import (
    RegistryMode,
    component_meta,
    component_registry,
    get_registry_mode,
    public_component_types,
)
from etl_core.components.base_component import Component
from etl_core.persistance.base_models.job_base import JobBase

router = APIRouter(prefix="/configs", tags=["configs"])

# Caches (thread-safe in-process)
_JOB_SCHEMA_CACHE: Dict[str, Dict[str, Any]] = {}
# key: (comp_type, mode)
_COMPONENT_SCHEMA_FORM_CACHE: Dict[Tuple[str, str], Dict[str, Any]] = {}
_COMPONENT_SCHEMA_FULL_CACHE: Dict[Tuple[str, str], Dict[str, Any]] = {}
_COMPONENT_SCHEMA_HIDDEN_CACHE: Dict[Tuple[str, str], Dict[str, Any]] = {}
_COMPONENT_TYPES_CACHE: Dict[str, List[str]] = {}

_CACHE_LOCK = RLock()


def invalidate_schema_caches() -> None:
    """Clear all schema caches. Call after changing registry mode/registry."""
    with _CACHE_LOCK:
        _JOB_SCHEMA_CACHE.clear()
        _COMPONENT_SCHEMA_FORM_CACHE.clear()
        _COMPONENT_SCHEMA_FULL_CACHE.clear()
        _COMPONENT_SCHEMA_HIDDEN_CACHE.clear()
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


def _hidden_fields_for_class(cls: type) -> set[str]:
    """
    Inspect Pydantic v2 model fields and return those marked as hidden for UI
    via json_schema_extra={'used_in_table': False}.
    """
    hidden: set[str] = set()
    for name, f in getattr(cls, "model_fields", {}).items():
        extra = getattr(f, "json_schema_extra", None) or {}
        if extra.get("used_in_table") is False:
            hidden.add(name)
    return hidden


def _strip_hidden(schema: Dict[str, Any], hidden: set[str]) -> Dict[str, Any]:
    """Remove hidden properties/required entries from a model JSON Schema."""
    if not hidden:
        return schema
    props = schema.get("properties")
    if isinstance(props, dict):
        for k in list(props.keys()):
            if k in hidden:
                props.pop(k, None)
    req = schema.get("required")
    if isinstance(req, list):
        schema["required"] = [k for k in req if k not in hidden]
    return schema


def _keep_only_hidden(schema: Dict[str, Any], hidden: set[str]) -> Dict[str, Any]:
    """
    Keep only hidden properties/required entries from a model JSON Schema.
    Produces an object schema with a subset of properties. If no hidden fields
    exist, returns an empty object schema.
    """
    props = schema.get("properties")
    out: Dict[str, Any] = {"type": "object", "properties": {}}
    if isinstance(props, dict) and hidden:
        kept: Dict[str, Any] = {k: v for k, v in props.items() if k in hidden}
        out["properties"] = kept
        req = schema.get("required")
        if isinstance(req, list):
            out["required"] = [k for k in req if k in hidden]
    return out


def _dump_spec(obj: Any) -> Dict[str, Any]:
    """
    Best-effort conversion of OutPortSpec/InPortSpec (or other small objects)
    into plain dicts for transport to the GUI.
    """
    # Pydantic models: model_dump
    dump = getattr(obj, "model_dump", None)
    if callable(dump):
        return dump()  # type: ignore[no-any-return]
    # dataclasses
    try:
        from dataclasses import asdict, is_dataclass

        if is_dataclass(obj):
            return asdict(obj)  # type: ignore[no-any-return]
    except Exception:
        pass
    # Fallback: shallow vars()
    try:
        return {k: v for k, v in vars(obj).items() if not k.startswith("_")}
    except Exception:
        return {"value": repr(obj)}


def _class_vars_payload(cls: Type[Component]) -> Dict[str, Any]:
    """
    Extract class-level declarations useful to the GUI.
    """
    # ClassVar sequences may be tuples; turn into serializable lists of dicts
    input_specs = [_dump_spec(p) for p in getattr(cls, "INPUT_PORTS", ()) or ()]
    output_specs = [_dump_spec(p) for p in getattr(cls, "OUTPUT_PORTS", ()) or ()]
    allow_no_inputs: bool = bool(getattr(cls, "ALLOW_NO_INPUTS", False))
    path_sep: str = getattr(cls, "_schema_path_separator", ".")
    return {
        "input_ports": input_specs,
        "output_ports": output_specs,
        "allow_no_inputs": allow_no_inputs,
        "schema_path_separator": path_sep,
        "input_port_names": [p.get("name") for p in input_specs if isinstance(p, dict)],
        "output_port_names": [
            p.get("name") for p in output_specs if isinstance(p, dict)
        ],
    }


def _attach_class_vars(schema: Dict[str, Any], cls: Type[Component]) -> Dict[str, Any]:
    """
    Add a stable extension key with useful class-level info for the GUI.
    """
    enriched = dict(schema)
    enriched["x-class"] = _class_vars_payload(cls)
    return enriched


def _resolve_component_class(comp_type: str) -> Type[Component]:
    mode = get_registry_mode()
    mode_key = getattr(mode, "value", str(mode))

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
    cls: Optional[Type[Component]] = component_registry.get(comp_type)
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
    return cls


def _cached_component_schema_form(comp_type: str) -> Dict[str, Any]:
    """
    Current behavior: form-focused schema (hidden fields stripped),
    with class vars attached.
    """
    mode = get_registry_mode()
    mode_key = getattr(mode, "value", str(mode))
    cache_key = (comp_type, mode_key)

    with _CACHE_LOCK:
        hit = _COMPONENT_SCHEMA_FORM_CACHE.get(cache_key)
        if hit is not None:
            return hit

    cls = _resolve_component_class(comp_type)
    full = cls.model_json_schema()
    hidden = _hidden_fields_for_class(cls)
    filtered = _strip_hidden(full, hidden)
    enriched = _attach_class_vars(filtered, cls)

    with _CACHE_LOCK:
        _COMPONENT_SCHEMA_FORM_CACHE[cache_key] = enriched
    return enriched


def _cached_component_schema_full(comp_type: str) -> Dict[str, Any]:
    """
    Full component schema, nothing stripped, class vars attached.
    """
    mode = get_registry_mode()
    mode_key = getattr(mode, "value", str(mode))
    cache_key = (comp_type, mode_key)

    with _CACHE_LOCK:
        hit = _COMPONENT_SCHEMA_FULL_CACHE.get(cache_key)
        if hit is not None:
            return hit

    cls = _resolve_component_class(comp_type)
    full = cls.model_json_schema()
    enriched = _attach_class_vars(full, cls)

    with _CACHE_LOCK:
        _COMPONENT_SCHEMA_FULL_CACHE[cache_key] = enriched
    return enriched


def _cached_component_schema_hidden(comp_type: str) -> Dict[str, Any]:
    """
    Hidden-only schema (only fields marked used_in_table=False),
    with class vars attached.
    """
    mode = get_registry_mode()
    mode_key = getattr(mode, "value", str(mode))
    cache_key = (comp_type, mode_key)

    with _CACHE_LOCK:
        hit = _COMPONENT_SCHEMA_HIDDEN_CACHE.get(cache_key)
        if hit is not None:
            return hit

    cls = _resolve_component_class(comp_type)
    full = cls.model_json_schema()
    hidden = _hidden_fields_for_class(cls)
    only_hidden = _keep_only_hidden(full, hidden)
    enriched = _attach_class_vars(only_hidden, cls)

    with _CACHE_LOCK:
        _COMPONENT_SCHEMA_HIDDEN_CACHE[cache_key] = enriched
    return enriched


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
    "/{comp_type}/form",
    response_model=dict,
    summary="Get form-focused component schema (hidden fields removed)",
)
def get_component_schema(comp_type: str) -> Dict[str, Any]:
    try:
        return _cached_component_schema_form(comp_type)
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


@router.get(
    "/{comp_type}/full",
    response_model=dict,
    summary="Get full component schema (no fields stripped)",
)
def get_component_schema_full(comp_type: str) -> Dict[str, Any]:
    try:
        return _cached_component_schema_full(comp_type)
    except HTTPException:
        raise
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=_error_payload(
                "SCHEMA_COMPONENT_FULL_INVALID",
                f"Full component {comp_type!r} schema validation failed.",
                comp_type=comp_type,
                **_exc_meta(exc),
            ),
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_error_payload(
                "SCHEMA_COMPONENT_FULL_GENERATION_FAILED",
                f"Failed to generate full JSON Schema for component {comp_type!r}.",
                comp_type=comp_type,
                **_exc_meta(exc),
            ),
        ) from exc


@router.get(
    "/{comp_type}/hidden",
    response_model=dict,
    summary="Get hidden-only component schema (GUI-hidden fields only)",
)
def get_component_schema_hidden(comp_type: str) -> Dict[str, Any]:
    try:
        return _cached_component_schema_hidden(comp_type)
    except HTTPException:
        raise
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=_error_payload(
                "SCHEMA_COMPONENT_HIDDEN_INVALID",
                f"Hidden-only component {comp_type!r} schema validation failed.",
                comp_type=comp_type,
                **_exc_meta(exc),
            ),
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_error_payload(
                "SCHEMA_COMPONENT_HIDDEN_GENERATION_FAILED",
                f"Failed to generate hidden-only JSON Schema "
                f"for component {comp_type!r}.",
                comp_type=comp_type,
                **_exc_meta(exc),
            ),
        ) from exc
