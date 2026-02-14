from __future__ import annotations

from typing import Any, Dict, List, Optional
from fastapi import HTTPException, status


def _payload(code: str, msg: str, **extra: Any) -> Dict[str, Any]:
    """Build error detail with code, message, and optional context fields"""
    return {"code": code, "message": msg, **extra}


def _exc_meta(exc: BaseException) -> Dict[str, Optional[str]]:
    """
    Best-effort, safe metadata from an exception (no stack traces)
    """
    return {
        "type": exc.__class__.__name__,
        "cause": str(exc.__cause__) if exc.__cause__ else None,
        "context": str(exc.__context__) if exc.__context__ else None,
    }


def _sanitize_errors(exc: Any) -> List[Dict[str, Any]]:
    """
    Sanitize Pydantic ValidationError details for API responses.
    Filters typical keys from error dicts.
    """
    errors = getattr(exc, "errors", lambda: [])()
    sanitized: List[Dict[str, Any]] = []
    for err in errors:
        filtered: Dict[str, Any] = {}
        for key in ("type", "loc", "msg", "url"):
            if key in err:
                filtered[key] = err[key]
        sanitized.append(filtered)
    return sanitized


# --- Simple HTTP error factories ---


def http_404(code: str, msg: str, **extra: Any) -> HTTPException:
    """Create a 404 Not Found HTTPException with structured detail."""
    return HTTPException(status.HTTP_404_NOT_FOUND, detail=_payload(code, msg, **extra))


def http_409(code: str, msg: str, **extra: Any) -> HTTPException:
    """Create a 409 Conflict HTTPException with structured detail."""
    return HTTPException(status.HTTP_409_CONFLICT, detail=_payload(code, msg, **extra))


def http_422(code: str, msg: str, **extra: Any) -> HTTPException:
    """Create a 422 Unprocessable Entity HTTPException with structured detail."""
    return HTTPException(
        status.HTTP_422_UNPROCESSABLE_ENTITY, detail=_payload(code, msg, **extra)
    )


def http_500(code: str, msg: str, **extra: Any) -> HTTPException:
    """Create a 500 Internal Server Error HTTPException with structured detail."""
    return HTTPException(
        status.HTTP_500_INTERNAL_SERVER_ERROR, detail=_payload(code, msg, **extra)
    )


def http_400(code: str, msg: str, **extra: Any) -> HTTPException:
    """Create a 400 Bad Request HTTPException with structured detail."""
    return HTTPException(
        status.HTTP_400_BAD_REQUEST, detail=_payload(code, msg, **extra)
    )


# --- HTTP error factories with exception metadata ---


def http_404_exc(
    code: str, msg: str, exc: BaseException, **extra: Any
) -> HTTPException:
    """Create a 404 with exception metadata merged into detail."""
    return http_404(code, msg, **extra, **_exc_meta(exc))


def http_409_exc(
    code: str, msg: str, exc: BaseException, **extra: Any
) -> HTTPException:
    """Create a 409 with exception metadata merged into detail."""
    return http_409(code, msg, **extra, **_exc_meta(exc))


def http_422_exc(
    code: str, msg: str, exc: BaseException, **extra: Any
) -> HTTPException:
    """Create a 422 with exception metadata merged into detail."""
    return http_422(code, msg, **extra, **_exc_meta(exc))


def http_500_exc(
    code: str, msg: str, exc: BaseException, **extra: Any
) -> HTTPException:
    """Create a 500 with exception metadata merged into detail."""
    return http_500(code, msg, **extra, **_exc_meta(exc))


def http_400_exc(
    code: str, msg: str, exc: BaseException, **extra: Any
) -> HTTPException:
    """Create a 400 with exception metadata merged into detail."""
    return http_400(code, msg, **extra, **_exc_meta(exc))


# --- Validation error helpers ---


def http_422_validation(
    code: str,
    msg: str,
    exc: BaseException,
    **extra: Any,
) -> HTTPException:
    """
    Create a 422 for Pydantic ValidationError with sanitized errors list
    and exception metadata.
    """
    return http_422(code, msg, errors=_sanitize_errors(exc), **extra, **_exc_meta(exc))
