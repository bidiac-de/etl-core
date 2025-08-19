from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field, model_validator
from src.context.context_provider import IContextProvider
from src.context.context_parameter import ContextParameter
from src.context.environment import Environment
from src.context.credentials import Credentials


class Context(BaseModel, IContextProvider):
    """
    Pydantic version of Context.

    Keeps the old public API:
      - properties: id, name, environment, parameters
      - `parameters` can be passed as a list or as a dict keyed by `key`
      - `get_parameter(key)` returns the parameter's value
      - `set_parameter(key, value)` updates an existing parameter
    """

    model_config = ConfigDict(
        extra="ignore",
        validate_assignment=True,
    )

    id: int
    name: str
    environment: Environment
    parameters: Dict[str, ContextParameter] = Field(default_factory=dict)

    # Add credentials storage
    _credentials: Dict[int, Credentials] = {}

    @model_validator(mode="before")
    @classmethod
    def _normalize_params(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Allow `parameters` to be provided as a list[ContextParameter] and turn it
        into a dict keyed by `key` (matching the old constructor semantics).
        """
        params = values.get("parameters")
        if params is None:
            return values

        if isinstance(params, dict):
            return values

        if isinstance(params, list):
            values["parameters"] = {p.key: p for p in params}
            return values

        raise TypeError(
            "parameters must be a dict[str, ContextParameter] or a "
            "list[ContextParameter]"
        )

    def get_parameter(self, key: str) -> str:
        try:
            return self.parameters[key].value
        except KeyError as exc:  # pragma: no cover - parity with old message
            raise KeyError(
                f"Parameter with key '{key}' not found in context '{self.name}'"
            ) from exc

    def set_parameter(self, key: str, value: str) -> None:
        if key in self.parameters:
            self.parameters[key].value = value
        else:
            raise KeyError(f"Cannot set value, parameter with key '{key}' not found.")

    def add_credentials(self, credentials: Credentials) -> None:
        """Add credentials to the context."""
        self._credentials[credentials.credentials_id] = credentials

    def get_credentials(self, credentials_id: int) -> Credentials:
        """Get credentials by ID."""
        if credentials_id not in self._credentials:
            raise KeyError(f"Credentials with ID {credentials_id} not found")
        return self._credentials[credentials_id]
