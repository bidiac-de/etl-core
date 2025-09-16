from __future__ import annotations

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from . import get_token_service
from .service import TokenPayload, TokenValidationError

_oauth2_bearer = HTTPBearer(auto_error=True)


def oauth2_scheme(
    credentials: HTTPAuthorizationCredentials = Depends(_oauth2_bearer),
) -> str:
    return credentials.credentials


async def require_authorized_client(
    token: str = Depends(oauth2_scheme),
) -> TokenPayload:
    service = get_token_service()
    try:
        return service.validate_token(token)
    except TokenValidationError as exc:  # pragma: no cover - exceptions propagate
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc


__all__ = [
    "oauth2_scheme",
    "require_authorized_client",
]
