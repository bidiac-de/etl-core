from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ContextParameter(BaseModel):
    """
    Pydantic version of ContextParameter with the same attributes and rules.
    """

    model_config = ConfigDict(
        extra="ignore",
        validate_assignment=True,
    )

    id: int = Field(..., gt=0)
    key: str = Field(..., min_length=1)
    value: str
    type: str
    is_secure: bool

    @field_validator("key")
    @classmethod
    def _non_empty_key(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Key cannot be empty")
        return v.strip()
