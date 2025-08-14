from typing import Annotated, Dict, List, Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from src.api.dependencies import get_job_handler
from src.api.helpers import _error_payload, _exc_meta, _sanitize_errors
from src.persistance.errors import (
    PersistLinkageError,
    PersistNotFoundError,
)
from src.persistance.configs.job_config import JobConfig
from src.persistance.handlers.job_handler import JobHandler

router = APIRouter(prefix="/jobs", tags=["Jobs"])


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
    try:
        job = job_handler.load_runtime_job(job_id)
    except PersistNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "JOB_NOT_FOUND", "message": str(exc)},
        ) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"code": "DB_ERROR", "message": "Failed to load job."},
        ) from exc

    data = job.model_dump()
    data["id"] = job.id
    return data


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
            detail=_error_payload(
                "JOB_NOT_FOUND",
                str(not_found),
                job_id=job_id,
            ),
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
    try:
        return job_handler.list_jobs_brief()
    except SQLAlchemyError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_error_payload(
                "DB_ERROR",
                "Failed to list jobs.",
                **_exc_meta(exc),
            ),
        ) from exc
