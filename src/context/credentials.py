from __future__ import annotations

import base64
import os
from typing import Any

from cryptography.fernet import Fernet
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator
from src.context.context_provider import IContextProvider


class Credentials(BaseModel, IContextProvider):
    """
    Pydantic model for database/login credentials with in-memory encryption,
    mirroring the original behavior.

    JSON input/output:
      - Accepts a `password` field as plain text (string).
      - On initialization, the password is encrypted into a private attribute and
        the public `password` field is cleared (set to None) so it never dumps out.
      - `get_parameter("password")` returns the decrypted password string.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
        validate_assignment=True,
        frozen=False,
    )

    credentials_id: int
    user: str
    database: str
    # Accept incoming password for schema/validation, but never expose on dump.
    password: SecretStr | None = Field(default=None, repr=False)

    # Private encryption bits (not in schema / not dumped)
    _cipher: Fernet = Field(default=None, repr=False)  # type: ignore[assignment]
    _password_encrypted: bytes | None = Field(default=None, repr=False)

    @field_validator("credentials_id")
    @classmethod
    def _validate_id(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("credentials_id must be positive")
        return v

    def model_post_init(self, __context: Any) -> None:  # noqa: D401
        """
        After init:
        - create a one-off cipher key
        - encrypt the provided password (if any)
        - wipe the public `password` field to avoid persistence/leakage
        """
        key = base64.urlsafe_b64encode(os.urandom(32))
        object.__setattr__(self, "_cipher", Fernet(key))

        if self.password is not None:
            encrypted = self._cipher.encrypt(
                self.password.get_secret_value().encode("utf-8")
            )
            object.__setattr__(self, "_password_encrypted", encrypted)
            # wipe public field
            object.__setattr__(self, "password", None)

    def __repr__(self) -> str:  # pragma: no cover - representational
        return (
            f"Credentials(credentials_id={self.credentials_id}, "
            f"user={self.user}, password=***, database={self.database})"
        )

    @property
    def decrypted_password(self) -> str:
        if self._password_encrypted is None:
            return ""
        return self._cipher.decrypt(self._password_encrypted).decode("utf-8")

    def get_parameter(self, key: str) -> Any:
        """
        Return a parameter by key, preserving the old mapping semantics.
        Supported keys: 'credentials_id', 'user', 'password', 'database'.
        """
        mapping: dict[str, Any] = {
            "credentials_id": self.credentials_id,
            "user": self.user,
            "password": self.decrypted_password,
            "database": self.database,
        }
        if key not in mapping:
            raise KeyError(f"Unknown parameter key: {key}")
        return mapping[key]
