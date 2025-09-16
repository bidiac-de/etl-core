"""Security utilities for OAuth2 client credentials and JWT handling."""

from __future__ import annotations

import os
from starlette.config import Config

from .config import AuthSettings, load_auth_settings
from .service import TokenService

_AUTH_SETTINGS: AuthSettings | None = None
_TOKEN_SERVICE: TokenService | None = None


def _apply_settings(settings: AuthSettings) -> None:
    global _AUTH_SETTINGS, _TOKEN_SERVICE
    _AUTH_SETTINGS = settings
    _TOKEN_SERVICE = TokenService(settings)


def _ensure_configured() -> None:
    """Initialise security configuration if it has not happened yet."""

    if _AUTH_SETTINGS is not None and _TOKEN_SERVICE is not None:
        return

    # Mirror the default bootstrap path used by the FastAPI application.
    env_path = os.environ.get("ETL_CORE_ENV_FILE", ".env")
    config = Config(env_path)
    settings = load_auth_settings(config)
    _apply_settings(settings)


def configure_security(config: Config) -> None:
    """Load auth settings and initialize the token service."""

    settings = load_auth_settings(config)
    _apply_settings(settings)


def get_auth_settings() -> AuthSettings:
    _ensure_configured()
    assert _AUTH_SETTINGS is not None  # for type checkers
    return _AUTH_SETTINGS


def get_token_service() -> TokenService:
    _ensure_configured()
    assert _TOKEN_SERVICE is not None
    return _TOKEN_SERVICE


__all__ = [
    "configure_security",
    "get_auth_settings",
    "get_token_service",
]
