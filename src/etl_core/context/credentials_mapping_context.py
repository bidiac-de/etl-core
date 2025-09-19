from typing import Dict, Optional, Any, Tuple
from pydantic import Field, model_validator
import os
from etl_core.context.context import Context
from etl_core.context.environment import Environment
from etl_core.context.credentials import Credentials
from etl_core.persistence.handlers.credentials_handler import CredentialsHandler


class CredentialsMappingContext(Context):
    """
    Wrapper context that maps env -> credentials_id.
    Inherits Context so it is swappable anywhere a Context is expected.
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
        # Accept Environment or str keys, store as strings (e.g., "TEST")
        normalized: Dict[str, str] = {}
        for k, v in mapping.items():
            if isinstance(k, Environment):
                normalized[k.value] = v
            else:
                normalized[str(k)] = v
        values["credentials_ids"] = normalized
        return values

    def _lookup_credentials_id(self, env: Environment) -> Optional[str]:
        key = env.value
        return self.credentials_ids.get(key)

    def resolve_active_credentials(
        self,
        override_env: Optional[Environment] = None,
        repo: Optional[CredentialsHandler] = None,
    ) -> Tuple[Credentials, str]:
        env = self.determine_active_environment(override_env)
        cred_id = self._lookup_credentials_id(env)
        if cred_id is None:
            raise ValueError(
                f"No credentials configured for env '{env.value}' in context "
                f"'{self.name}'."
            )
        repository = repo or self._credentials_repo or CredentialsHandler()
        loaded = repository.get_by_id(cred_id)
        if not loaded:
            raise ValueError(f"Credentials with ID {cred_id} not found")
        creds, _ = loaded
        self.add_credentials(cred_id, creds)
        return creds, cred_id

    @staticmethod
    def _normalize_env(env: Environment | str) -> Environment:
        if isinstance(env, Environment):
            return env
        return Environment(env.strip())

    def determine_active_environment(
        self, override: Optional[Environment] = None
    ) -> Environment:
        if override is not None:
            return self._normalize_env(override)
        env_from_os = os.getenv("EXECUTION_ENV")
        if env_from_os:
            return self._normalize_env(env_from_os)
        return self.environment
