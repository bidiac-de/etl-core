from __future__ import annotations

from typing import Any, Dict

from fastapi import HTTPException

from etl_core.errors import ETLCoreError


def canonical_error_payload(
    code: str,
    message: str,
    *,
    details: list[dict[str, Any]] | None = None,
    context: dict[str, Any] | None = None,
) -> Dict[str, Any]:
    return {
        "error": {
            "code": code,
            "message": message,
            "details": details or [],
            "context": context or {},
        }
    }


def etl_error_to_http_exception(exc: ETLCoreError) -> HTTPException:
    """
    Convert typed ETLCoreError into HTTPException detail shape expected by the
    global Starlette/FastAPI HTTP exception handler.
    """

    detail: Dict[str, Any] = {
        "code": exc.code,
        "message": exc.message,
        "errors": exc.details,
        "context": exc.context,
    }
    return HTTPException(status_code=exc.http_status, detail=detail)

