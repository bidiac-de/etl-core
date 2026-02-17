from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator
from etl_core.context.context_provider import IContextProvider
from etl_core.context.context_parameter import ContextParameter
from etl_core.context.environment import normalize_environment
from etl_core.context.credentials import Credentials
from etl_core.persistence.handlers.credentials_handler import CredentialsHandler


class Context(BaseModel, IContextProvider):
    """
    public API:
      - properties: id, name, environment, parameters
      - `parameters` can be passed as a list or as a dict keyed by `key`
      - `get_parameter(key)` returns the parameter's value
      - `set_parameter(key, value)` updates an existing parameter

    The ``environment`` field accepts an ``Environment`` enum member **or**
    any non-empty string (e.g. ``"STAGING"``, ``"UAT"``).  It is always
    stored as an uppercase string.
    """

    model_config = ConfigDict(
        extra="ignore",
        validate_assignment=True,
    )

    name: str
    environment: str
    parameters: Dict[str, ContextParameter] = Field(default_factory=dict)

    _credentials: Dict[str, Credentials] = {}
    _credentials_repo: Optional[CredentialsHandler] = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_params(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        - Normalise ``environment`` to an uppercase string (accepts
          ``Environment`` enum or plain str).
        - Allow ``parameters`` to be provided as a list[ContextParameter]
          and turn it into a dict keyed by ``key``.
        """
        # --- environment ---
        env = values.get("environment")
        if env is not None:
            values["environment"] = normalize_environment(env)

        # --- parameters ---
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

    def attach_credentials_repository(self, repo: CredentialsHandler) -> None:
        self._credentials_repo = repo

    def add_credentials(self, cred_id: str, credentials: Credentials) -> None:
        """Add credentials to the context."""
        self._credentials[cred_id] = credentials

    def get_credentials(self, credentials_id: str) -> Credentials:
        cached = self._credentials.get(credentials_id)
        if cached is not None:
            return cached
        if self._credentials_repo is None:
            raise KeyError(f"Credentials with ID {credentials_id} not found")
        loaded = self._credentials_repo.get_by_id(credentials_id)
        if loaded is None:
            raise KeyError(f"Credentials with ID {credentials_id} not found")
        creds, _provider_id = loaded
        self._credentials[credentials_id] = creds
        return creds
