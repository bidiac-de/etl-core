from __future__ import annotations

from typing import List, Literal

from fastapi import APIRouter, Depends, Form, HTTPException, status
from pydantic import BaseModel

from etl_core.security import get_auth_settings, get_token_service
from etl_core.security.service import AuthenticationError


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


class OAuth2ClientCredentialsRequestForm:
    def __init__(
        self,
        grant_type: Literal["client_credentials"] = Form(
            "client_credentials",
            description="OAuth2 grant type",
        ),
        scope: str = Form("", description="Requested scopes"),
        client_id: str | None = Form(
            None,
            description="OAuth2 client identifier (defaults to configured client).",
        ),
        client_secret: str = Form(..., description="OAuth2 client secret"),
    ) -> None:
        self.grant_type = grant_type
        self.scope = scope
        self.client_id = client_id
        self.client_secret = client_secret

    @property
    def scopes(self) -> List[str]:
        return [s for s in self.scope.split() if s]


router = APIRouter(prefix="/auth", tags=["Auth"])


@router.post(
    "/token",
    response_model=TokenResponse,
    summary="Issue OAuth2 access token",
    description=(
        "Exchange the configured client credentials for a JWT access token. "
        "Paste the returned token into the Swagger UI's Authorize dialog "
        "(HTTPBearer scheme) to call protected endpoints."
    ),
)
async def create_access_token(
    form_data: OAuth2ClientCredentialsRequestForm = Depends(),
) -> TokenResponse:
    if form_data.grant_type != "client_credentials":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only the client_credentials grant type is supported",
        )

    token_service = get_token_service()
    try:
        client = token_service.authenticate_client(
            client_secret=form_data.client_secret,
            client_id=form_data.client_id or None,
        )
    except AuthenticationError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid client credentials",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc

    token = token_service.issue_token(client=client, scopes=form_data.scopes)
    settings = get_auth_settings()
    return TokenResponse(
        access_token=token,
        expires_in=settings.token_expiry_seconds,
    )


__all__ = ["router"]
