from __future__ import annotations

from fastapi import APIRouter, Depends

from etl_core.security.dependencies import require_authorized_client


router = APIRouter(
    prefix="/health",
    tags=["Health"],
    dependencies=[Depends(require_authorized_client)],
)


@router.get("", summary="Service health status")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}


__all__ = ["router"]
