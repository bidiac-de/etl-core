from __future__ import annotations

import pytest
from starlette.config import Config

from etl_core.security.config import MIN_SECRET_LENGTH, load_auth_settings


def test_load_auth_settings_rejects_weak_secret() -> None:
    weak_secret = "aB1!" * 3
    config = Config(
        environ={
            "ETL_AUTH_CLIENT_SECRET": weak_secret,
            "ETL_AUTH_SIGNING_KEY": "StrongSigningKey!234567890ABCdef",
        }
    )
    with pytest.raises(RuntimeError):
        load_auth_settings(config)


def test_load_auth_settings_accepts_strong_secret() -> None:
    strong = "SecureClientSecret!234567890ABCdef"
    signing = "SigningKeyForJWT#9876543210AbCdEf!"
    config = Config(
        environ={
            "ETL_AUTH_CLIENT_SECRET": strong,
            "ETL_AUTH_SIGNING_KEY": signing,
            "ETL_AUTH_TOKEN_EXPIRES_IN": "120",
        }
    )
    settings = load_auth_settings(config)
    assert settings.client_secret == strong
    assert settings.signing_key == signing
    assert settings.token_expiry_seconds == 120
    assert len(strong) >= MIN_SECRET_LENGTH
