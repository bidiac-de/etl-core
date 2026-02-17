from typing import Dict, Optional, Any, Tuple
from pydantic import Field, model_validator
import os
from etl_core.context.context import Context
from etl_core.context.environment import normalize_environment
from etl_core.context.credentials import Credentials
from etl_core.persistence.handlers.credentials_handler import CredentialsHandler


class CredentialsMappingContext(Context):
    """
    Wrapper context that maps env -> credentials_id.
    Inherits Context so it is swappable anywhere a Context is expected.

    Environment keys in ``credentials_ids`` can now be **any** non-empty
    string (e.g. ``"STAGING"``), not just the built-in DEV/TEST/PROD.
    """

    # store as string keys to match DB rows (e.g., "TEST", "DEV", "PROD")
    credentials_ids: Dict[str, str] = Field(
        default_factory=dict, description="Mapping environment -> credentials_id."
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_map_keys(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        mapping = values.get("credentials_ids")
        if not mapping:
            return values
        # Accept Environment enum or str keys, normalise to uppercase strings
        normalized: Dict[str, str] = {}
        for k, v in mapping.items():
            normalized[normalize_environment(k)] = v
        values["credentials_ids"] = normalized
        return values

    def _lookup_credentials_id(self, env: str) -> Optional[str]:
        key = normalize_environment(env)
        return self.credentials_ids.get(key)

    def resolve_active_credentials(
        self,
        override_env: Optional[str] = None,
        repo: Optional[CredentialsHandler] = None,
    ) -> Tuple[Credentials, str]:
        env = self.determine_active_environment(override_env)
        cred_id = self._lookup_credentials_id(env)
        if cred_id is None:
            raise ValueError(
                f"No credentials configured for env '{env}' in context "
                f"'{self.name}'."
            )
        repository = repo or self._credentials_repo or CredentialsHandler()
        loaded = repository.get_by_id(cred_id)
        if not loaded:
            raise ValueError(f"Credentials with ID {cred_id} not found")
        creds, _ = loaded
        self.add_credentials(cred_id, creds)
        return creds, cred_id

    def determine_active_environment(self, override: Optional[str] = None) -> str:
        """Return the active environment as a normalised uppercase string."""
        if override is not None:
            return normalize_environment(override)
        env_from_os = os.getenv("EXECUTION_ENV")
        if env_from_os:
            return normalize_environment(env_from_os)
        return normalize_environment(self.environment)
