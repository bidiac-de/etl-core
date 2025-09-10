from typing import Dict, Optional
from pydantic import Field
import os
from etl_core.context.context import Context
from etl_core.context.environment import Environment
from etl_core.context.credentials import Credentials
from etl_core.persistance.handlers.credentials_handler import CredentialsHandler


class CredentialsMappingContext(Context):
    """
    Wrapper context that maps env -> credentials_id.
    Inherits Context so it is swappable anywhere a Context is expected.
    """

    credentials_ids: Dict[Environment, str] = Field(
        default_factory=dict, description="Mapping environment -> credentials_id."
    )

    def _lookup_credentials_id(self, env: Environment) -> Optional[str]:
        key = env.value
        if key in self.credentials_ids:
            return self.credentials_ids[key]
        return None

    def resolve_active_credentials(
        self,
        override_env: Optional[Environment] = None,
        repo: Optional[CredentialsHandler] = None,
    ) -> Credentials:
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
        self.add_credentials(creds)
        return creds

    @staticmethod
    def _normalize_env(env: Environment | str) -> Environment:
        if isinstance(env, Environment):
            return env
        return Environment(env.strip())

    def determine_active_environment(
        self, override: Optional[Environment] = None
    ) -> Environment:
        """
        Order: explicit override -> COMP_ENV -> this Context.environment
        """
        if override is not None:
            return self._normalize_env(override)
        env_from_os = os.getenv("COMP_ENV")
        if env_from_os:
            return self._normalize_env(env_from_os)
        return self.environment
