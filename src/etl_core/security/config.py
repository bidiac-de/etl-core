from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from starlette.config import Config

logger = logging.getLogger(__name__)

MIN_SECRET_LENGTH = 32


@dataclass(frozen=True)
class AuthSettings:
    client_id: str
    client_secret: str
    signing_key: str
    token_expiry_seconds: int
    issuer: str
    audience: str


def _evaluate_secret_strength(secret: str) -> list[str]:
    issues: list[str] = []
    if len(secret) < MIN_SECRET_LENGTH:
        issues.append(
            f"must be at least {MIN_SECRET_LENGTH} characters long"
        )
    if secret.islower() or secret.isupper():
        issues.append("must include a mix of upper and lower case characters")
    if not any(ch.isdigit() for ch in secret):
        issues.append("must include at least one digit")
    if not re.search(r"[^a-zA-Z0-9]", secret):
        issues.append("must include at least one symbol")
    if secret.strip() != secret:
        issues.append("must not contain leading or trailing whitespace")
    if len(set(secret)) < 8:
        issues.append("must contain a diverse set of characters")
    return issues


def _require_secret(name: str, value: str) -> str:
    if not value or value.strip() == "":
        raise RuntimeError(f"{name} environment variable is required")
    issues = _evaluate_secret_strength(value)
    if issues:
        raise RuntimeError(
            f"{name} does not meet minimum strength requirements: {', '.join(issues)}"
        )
    return value


def load_auth_settings(config: Config) -> AuthSettings:
    client_id = config("ETL_AUTH_CLIENT_ID", cast=str, default="etl-core-client")
    try:
        raw_client_secret = config("ETL_AUTH_CLIENT_SECRET", cast=str)
    except KeyError as exc:  # pragma: no cover - defensive
        raise RuntimeError("ETL_AUTH_CLIENT_SECRET environment variable is required") from exc
    client_secret = _require_secret("ETL_AUTH_CLIENT_SECRET", raw_client_secret)

    try:
        raw_signing_key = config("ETL_AUTH_SIGNING_KEY", cast=str)
    except KeyError as exc:  # pragma: no cover - defensive
        raise RuntimeError("ETL_AUTH_SIGNING_KEY environment variable is required") from exc
    signing_key = _require_secret("ETL_AUTH_SIGNING_KEY", raw_signing_key)
    token_expiry_seconds = config(
        "ETL_AUTH_TOKEN_EXPIRES_IN", cast=int, default=3600
    )
    if token_expiry_seconds <= 0:
        raise RuntimeError("ETL_AUTH_TOKEN_EXPIRES_IN must be positive")

    issuer = config("ETL_AUTH_TOKEN_ISSUER", cast=str, default="etl-core")
    audience = config(
        "ETL_AUTH_TOKEN_AUDIENCE", cast=str, default=client_id
    )

    logger.info(
        "OAuth2 security configured with issuer=%s and audience=%s",
        issuer,
        audience,
    )

    return AuthSettings(
        client_id=client_id,
        client_secret=client_secret,
        signing_key=signing_key,
        token_expiry_seconds=token_expiry_seconds,
        issuer=issuer,
        audience=audience,
    )


__all__ = ["AuthSettings", "load_auth_settings", "MIN_SECRET_LENGTH"]
