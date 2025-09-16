from __future__ import annotations

from threading import RLock
from typing import Annotated, Dict, List, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from etl_core.api.dependencies import get_job_handler
from etl_core.api.helpers import _error_payload, _exc_meta, _sanitize_errors
from etl_core.persistence.errors import (
    PersistLinkageError,
    PersistNotFoundError,
)
from etl_core.persistence.configs.job_config import JobConfig
from etl_core.persistence.handlers.job_handler import JobHandler
from etl_core.security.dependencies import require_authorized_client

router = APIRouter(
    prefix="/jobs",
    tags=["Jobs"],
    dependencies=[Depends(require_authorized_client)],
)

_JOB_BY_ID_CACHE: Dict[str, Dict[str, Any]] = {}
_JOB_LIST_CACHE: Optional[List[Dict[str, Any]]] = None
_CACHE_LOCK = RLock()


def invalidate_job_caches(job_id: Optional[str] = None) -> None:
    """
    Clear GET caches for /jobs. If job_id is provided, drop only that entry
    in the by-id cache; the list cache is always cleared because its contents
    depend on the full set of jobs.
    """
    global _JOB_LIST_CACHE
    with _CACHE_LOCK:
        if job_id is not None:
            _JOB_BY_ID_CACHE.pop(job_id, None)
        _JOB_LIST_CACHE = None


def _cached_job(job_id: str, job_handler: JobHandler) -> Dict[str, Any]:
    with _CACHE_LOCK:
        hit = _JOB_BY_ID_CACHE.get(job_id)
        if hit is not None:
            return hit

    # Compute outside of lock to reduce contention
    try:
        job = job_handler.load_runtime_job(job_id)
    except PersistNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "JOB_NOT_FOUND", "message": str(exc)},
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"code": "DB_ERROR", "message": "Failed to load job."},
        ) from exc

    data: Dict[str, Any] = job.model_dump()
    data["id"] = job.id

    with _CACHE_LOCK:
        _JOB_BY_ID_CACHE[job_id] = data
    return data


def _cached_job_list(job_handler: JobHandler) -> List[Dict[str, Any]]:
    global _JOB_LIST_CACHE
    with _CACHE_LOCK:
        if _JOB_LIST_CACHE is not None:
            # Return a copy to avoid accidental external mutation
            return list(_JOB_LIST_CACHE)

    try:
        rows = job_handler.list_jobs_brief()
    except SQLAlchemyError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_error_payload("DB_ERROR", "Failed to list jobs.", **_exc_meta(exc)),
        ) from exc

    # write back under lock after computing and return a copy
    with _CACHE_LOCK:
        _JOB_LIST_CACHE = list(rows)
        return list(_JOB_LIST_CACHE)


@router.post(
    "/",
    response_model=str,
    summary="Create a new Job",
    description="Creates a new Job with the provided configuration and persists it.",
)
def create_job(
    job_cfg: JobConfig,
    job_handler: Annotated[JobHandler, Depends(get_job_handler)],
) -> str:
    try:
        entry = job_handler.create_job_entry(job_cfg)
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=_error_payload(
                "JOB_CONFIG_INVALID",
                "JobConfig validation failed.",
                errors=_sanitize_errors(exc),
                **_exc_meta(exc),
            ),
        ) from exc
    except PersistLinkageError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=_error_payload(
                "COMPONENT_REFERENCE_INVALID",
                "Invalid component linkage in configuration.",
                **_exc_meta(exc),
            ),
        ) from exc
    except IntegrityError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=_error_payload(
                "DB_INTEGRITY_ERROR",
                "Database integrity error while persisting a job.",
                **_exc_meta(exc),
            ),
        ) from exc
    except SQLAlchemyError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_error_payload(
                "DB_ERROR",
                "Database error while persisting a job.",
                **_exc_meta(exc),
            ),
        ) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_error_payload(
                "JOB_PERSIST_FAILED",
                "Failed to persist job.",
                **_exc_meta(exc),
            ),
        ) from exc

    # New job changes listing --> clear caches.
    invalidate_job_caches()
    return entry.id


@router.get(
    "/{job_id}",
    response_model=Dict,
    summary="Get Job by ID",
    description="Returns the JSON representation of the Job matching the given ID.",
)
def get_job(
    job_id: str,
    job_handler: JobHandler = Depends(get_job_handler),
) -> Dict[str, Any]:
    # Fetch via cache wrapper, raising HTTPExceptions on errors
    return _cached_job(job_id, job_handler)


@router.put("/{job_id}", response_model=str)
def update_job(
    job_id: str,
    job_cfg: JobConfig,
    job_handler: Annotated[JobHandler, Depends(get_job_handler)],
) -> str:
    try:
        row = job_handler.update(job_id, job_cfg)
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=_error_payload(
                "JOB_CONFIG_INVALID",
                "JobConfig validation failed.",
                errors=_sanitize_errors(exc),
                **_exc_meta(exc),
            ),
        ) from exc
    except PersistNotFoundError as not_found:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=_error_payload("JOB_NOT_FOUND", str(not_found), job_id=job_id),
        ) from not_found
    except IntegrityError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=_error_payload(
                "DB_INTEGRITY_ERROR",
                "Database integrity error while updating a job.",
                job_id=job_id,
                **_exc_meta(exc),
            ),
        ) from exc
    except SQLAlchemyError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_error_payload(
                "DB_ERROR",
                "Database error while updating a job.",
                job_id=job_id,
                **_exc_meta(exc),
            ),
        ) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_error_payload(
                "JOB_UPDATE_FAILED",
                "Failed to update job.",
                job_id=job_id,
                **_exc_meta(exc),
            ),
        ) from exc

    # Updated job invalidates list and this jobs cache entry
    invalidate_job_caches(job_id)
    return row.id


@router.delete(
    "/{job_id}",
    response_model=Dict,
    summary="Delete Job by ID",
    description="Deletes the Job matching the given ID.",
)
def delete_job(
    job_id: str,
    job_handler: JobHandler = Depends(get_job_handler),
) -> Dict[str, str]:
    try:
        job_handler.delete(job_id)
    except PersistNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "JOB_NOT_FOUND", "message": str(exc)},
        ) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"code": "DB_ERROR", "message": "Failed to delete job."},
        ) from exc

    # Deletion changes listing and removes this id
    invalidate_job_caches(job_id)
    return {"message": f"Job {job_id!r} deleted successfully"}


@router.get(
    "/",
    response_model=List[Dict],
    summary="List all Jobs (no components)",
    description="Returns each job’s data—excluding components but including metadata.",
)
def list_jobs(
    job_handler: Annotated[JobHandler, Depends(get_job_handler)],
) -> List[Dict]:
    # Cached listing
    return _cached_job_list(job_handler)
