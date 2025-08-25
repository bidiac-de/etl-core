from __future__ import annotations

from typing import Any, Dict
from fastapi import HTTPException, status


def _payload(code: str, msg: str, **extra: Any) -> Dict[str, Any]:
    return {"code": code, "message": msg, **extra}


def http_404(code: str, msg: str, **extra: Any) -> HTTPException:
    return HTTPException(status.HTTP_404_NOT_FOUND, detail=_payload(code, msg, **extra))


def http_409(code: str, msg: str, **extra: Any) -> HTTPException:
    return HTTPException(status.HTTP_409_CONFLICT, detail=_payload(code, msg, **extra))


def http_422(code: str, msg: str, **extra: Any) -> HTTPException:
    return HTTPException(
        status.HTTP_422_UNPROCESSABLE_ENTITY, detail=_payload(code, msg, **extra)
    )


def http_500(code: str, msg: str, **extra: Any) -> HTTPException:
    return HTTPException(
        status.HTTP_500_INTERNAL_SERVER_ERROR, detail=_payload(code, msg, **extra)
    )
