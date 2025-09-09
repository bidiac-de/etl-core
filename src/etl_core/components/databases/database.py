from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Any, Dict, Mapping, Optional

import dask.dataframe as dd
import pandas as pd
from pydantic import Field, model_validator

from etl_core.components.base_component import Component
from etl_core.context.credentials import Credentials
from etl_core.context.environment import Environment
from etl_core.persistance.handlers.credentials_handler import CredentialsHandler


class DatabaseComponent(Component, ABC):
    """
    Base class for database components (Mongo, SQL, ...).

    Enforces multiple-credentials-by-environment:
      - credentials_by_env is REQUIRED; no legacy fallback.
      - The active environment is chosen in this order:
          (1) explicitly set via set_runtime_environment()
          (2) environment variable ETL_ENV (case-insensitive)
          (3) raise if neither is provided
      - Exactly one credential is resolved for the active env.

    Reusability:
      - credentials_id values are provider IDs from persistence, so the same
        credentials object can be referenced by many components.
    """

    credentials_ids: Dict[Environment, str] = Field(
        description="Mapping environment -> credentials_id."
    )

    entity_name: str = Field(
        default="",
        description="name of the target entity (table/collection).",
    )

    row_batch_size: int = Field(
        default=1_000,
        ge=1,
        description="Hint for server-side cursor batch size in row streaming.",
    )
    bulk_chunk_size: int = Field(
        default=50_000,
        ge=1,
        description="Records per chunk when writing/reading pandas DataFrames.",
    )
    bigdata_partition_chunk_size: int = Field(
        default=50_000,
        ge=1,
        description=(
            "Records per chunk inside each Dask partition for bigdata mode "
            "(used by readers/writers)."
        ),
    )

    _active_env: Optional[Environment] = None
    _active_credentials_id: Optional[str] = None
    _credentials: Optional[Credentials] = None
    _receiver: Any = None

    @model_validator(mode="after")
    def _build_objects(self) -> "DatabaseComponent":
        # Fail fast if mapping is empty
        if not self.credentials_ids:
            raise ValueError(
                "credentials_ids is required and must contain at least one entry."
            )
        # Resolve immediately so misconfigurations fail at init time.
        self._credentials = self._resolve_credentials()
        return self

    def _determine_active_env(self) -> Environment:
        """
        Decide which environment to use for credentials selection,
        based on env variable ETL_ENV.
        """
        if self._active_env is not None:
            return self._active_env

        env_from_os = os.getenv("COMP_ENV")
        if env_from_os:
            self._active_env = self._normalize_env(env_from_os)
            return self._active_env

        raise ValueError("Active environment is not defined. Set COMP_ENV")

    @staticmethod
    def _normalize_env(env: Environment | str) -> Environment:
        if isinstance(env, Environment):
            return env
        return Environment(env.strip())

    @staticmethod
    def _lookup_credentials_id(
        env: Environment, mapping: Mapping[str, str]
    ) -> Optional[str]:
        """
        Try common key casings for robustness against persisted variations.
        """
        key = env.value
        if key in mapping:
            return mapping[key]
        if key.upper() in mapping:
            return mapping[key.upper()]
        cap = key.capitalize()
        if cap in mapping:
            return mapping[cap]
        return None

    def _resolve_credentials(self) -> Credentials:
        env = self._determine_active_env()

        selected_id = self.credentials_ids.get(env)
        if selected_id is None:
            raise ValueError(
                f"No credentials configured for env '{env.value}'. "
                "Provide a credentials_id in credentials_ids for this env."
            )

        repo = CredentialsHandler()
        loaded = repo.get_by_id(selected_id)
        if not loaded:
            raise ValueError(f"Credentials with ID {selected_id} not found")

        creds, _ = loaded
        self._active_env = env
        self._active_credentials_id = selected_id
        return creds

    def _get_credentials(self) -> Dict[str, Any]:
        """
        Return connection parameters as a plain dict for receivers.
        Lazily resolves credentials if needed.
        """
        if self._credentials is None:
            self._credentials = self._resolve_credentials()

        creds = self._credentials
        return {
            "user": creds.get_parameter("user"),
            "password": creds.decrypted_password,
            "database": creds.get_parameter("database"),
            "host": creds.get_parameter("host"),
            "port": creds.get_parameter("port"),
            "__selected_env__": self._active_env.value if self._active_env else None,
            "__credentials_id__": self._active_credentials_id,
        }

    @abstractmethod
    async def process_row(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """Process a single row. Implement in subclass."""
        raise NotImplementedError

    @abstractmethod
    async def process_bulk(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """Process an in-memory batch. Implement in subclass."""
        raise NotImplementedError

    @abstractmethod
    async def process_bigdata(self, *args: Any, **kwargs: Any) -> dd.DataFrame:
        """
        Stream-processing for big data. Implement in subclass.
        Should be a generator to avoid materializing large data.
        """
        raise NotImplementedError
