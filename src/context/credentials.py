# src/context/credentials.py
from __future__ import annotations
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, SecretStr
from src.context.context_provider import IContextProvider


class Credentials(BaseModel, IContextProvider):
    """
    Pydantic model for database/login credentials with portable pool settings.
    Shared knobs:
      - pool_max_size: maximum connections per pool/client
      - pool_timeout_s: time to wait for a free connection (seconds)
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
        validate_assignment=True,
        frozen=False,
    )

    credentials_id: int
    name: str
    user: str
    database: str
    password: Optional[SecretStr] = Field(default=None, repr=False)

    # portable pool settings for sql and mongo connections
    pool_max_size: Optional[int] = Field(default=None, ge=1)
    pool_timeout_s: Optional[int] = Field(default=None, ge=0)

    @property
    def decrypted_password(self) -> Optional[str]:
        if self.password is None:
            return None
        return self.password.get_secret_value()

    def get_parameter(self, key: str) -> Any:
        mapping: dict[str, Any] = {
            "credentials_id": self.credentials_id,
            "user": self.user,
            "database": self.database,
            # expose portable pool settings via provider API
            "pool_max_size": self.pool_max_size,
            "pool_timeout_s": self.pool_timeout_s,
        }
        if key not in mapping:
            raise KeyError(f"Unknown parameter key: {key}")
        return mapping[key]
