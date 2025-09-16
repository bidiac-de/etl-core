from __future__ import annotations

import logging
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Sequence

import jwt
from jwt import InvalidTokenError

from .config import AuthSettings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AuthenticatedClient:
    client_id: str


@dataclass(frozen=True)
class TokenPayload:
    client_id: str
    scopes: tuple[str, ...]
    expires_at: datetime


class AuthenticationError(Exception):
    """Raised when client authentication fails."""


class TokenValidationError(Exception):
    """Raised when access token validation fails."""


class TokenService:
    def __init__(self, settings: AuthSettings) -> None:
        self._settings = settings

    @property
    def default_expiry(self) -> timedelta:
        return timedelta(seconds=self._settings.token_expiry_seconds)

    def authenticate_client(
        self, *, client_secret: str, client_id: str | None = None
    ) -> AuthenticatedClient:
        expected_id = self._settings.client_id
        if client_id and client_id != expected_id:
            logger.warning(
                "Authentication failed for unknown client_id=%s", client_id
            )
            raise AuthenticationError("Invalid client credentials")

        if not secrets.compare_digest(client_secret, self._settings.client_secret):
            logger.warning("Authentication failed for client_id=%s", expected_id)
            raise AuthenticationError("Invalid client credentials")

        logger.info("Client %s authenticated via client credentials flow", expected_id)
        return AuthenticatedClient(client_id=expected_id)

    def issue_token(
        self, *, client: AuthenticatedClient, scopes: Sequence[str] | None = None
    ) -> str:
        now = datetime.now(timezone.utc)
        expires = now + self.default_expiry
        scope_string = " ".join(sorted({s for s in (scopes or []) if s}))
        payload = {
            "sub": client.client_id,
            "iss": self._settings.issuer,
            "aud": self._settings.audience,
            "iat": int(now.timestamp()),
            "nbf": int(now.timestamp()),
            "exp": int(expires.timestamp()),
        }
        if scope_string:
            payload["scope"] = scope_string

        token = jwt.encode(payload, self._settings.signing_key, algorithm="HS256")
        logger.info(
            "Issued access token for client_id=%s scopes=%s",
            client.client_id,
            scope_string or "<none>",
        )
        return token

    def validate_token(self, token: str) -> TokenPayload:
        try:
            decoded = jwt.decode(
                token,
                self._settings.signing_key,
                algorithms=["HS256"],
                audience=self._settings.audience,
                issuer=self._settings.issuer,
            )
        except InvalidTokenError as exc:
            logger.warning("Rejected invalid access token: %s", exc)
            raise TokenValidationError("Invalid access token") from exc

        sub = decoded.get("sub")
        if not isinstance(sub, str) or not sub:
            raise TokenValidationError("Token missing subject")

        raw_scope = decoded.get("scope", "")
        if isinstance(raw_scope, str):
            scopes: Iterable[str] = [s for s in raw_scope.split() if s]
        elif raw_scope is None:
            scopes = []
        else:
            raise TokenValidationError("Token scope must be a string")

        exp = decoded.get("exp")
        if not isinstance(exp, (int, float)):
            raise TokenValidationError("Token expiry missing")
        expires_at = datetime.fromtimestamp(exp, tz=timezone.utc)

        return TokenPayload(
            client_id=sub,
            scopes=tuple(scopes),
            expires_at=expires_at,
        )


__all__ = [
    "AuthenticatedClient",
    "TokenPayload",
    "AuthenticationError",
    "TokenValidationError",
    "TokenService",
]
