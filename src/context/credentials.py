from __future__ import annotations
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, SecretStr
from src.context.context_provider import IContextProvider


class Credentials(BaseModel, IContextProvider):
    """
    Pydantic model for database/login credentials.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
        validate_assignment=True,
        frozen=False,
    )

    name: str
    user: str
    database: str
    password: Optional[SecretStr] = Field(default=None, repr=False)

    @property
    def decrypted_password(self) -> Optional[str]:
        return self.password.get_secret_value() if self.password is not None else None

    def get_parameter(self, key: str) -> Any:
        mapping: dict[str, Any] = {
            "credentials_id": self.credentials_id,
            "user": self.user,
            "database": self.database,
        }
        if key not in mapping:
            raise KeyError(f"Unknown parameter key: {key}")
        return mapping[key]
