from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import dask.dataframe as dd
import pandas as pd
from pydantic import Field, model_validator

from etl_core.components.base_component import Component
from etl_core.context.credentials import Credentials
from etl_core.context.credentials_mapping_context import CredentialsMappingContext
from etl_core.context.environment import normalize_environment
from etl_core.errors import ContextResolutionError, CredentialResolutionError


class DatabaseComponent(Component, ABC):
    """
    Base class for database components (Mongo, SQL, ...).

    Enforces multiple-credentials-by-environment:
      - The active environment is chosen in this order:
      - Exactly one credential is resolved for the active env.
      Reusability:
      - credentials_id values are provider IDs from persistence, so the same
        credentials object can be referenced by many components.
    """

    ICON = "fa-solid fa-database"

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

    _credentials: Optional[Credentials] = None
    _cred_id = None
    _receiver: Any = None

    def _resolve_credentials_for_environment(
        self, environment: Optional[str] = None
    ) -> None:
        ctx = self.get_resolved_context()
        if ctx is None:
            raise ContextResolutionError(
                f"{self.name}: Database components require a context_id referencing "
                "a CredentialsMappingContext.",
                code="DB_CONTEXT_REQUIRED",
                context={"component": self.name, "context_id": self.context_id},
            )
        if not isinstance(ctx, CredentialsMappingContext):
            raise ContextResolutionError(
                f"{self.name}: context must be a CredentialsMappingContext; got "
                f"{type(ctx).__name__}.",
                code="DB_CONTEXT_INVALID_TYPE",
                context={
                    "component": self.name,
                    "context_id": self.context_id,
                    "context_type": type(ctx).__name__,
                },
            )

        env_str: Optional[str] = None
        if environment is not None:
            env_str = normalize_environment(environment)
        try:
            self._credentials, self._cred_id = ctx.resolve_active_credentials(
                override_env=env_str
            )
        except Exception as exc:  # noqa: BLE001
            raise CredentialResolutionError(
                f"{self.name}: failed to resolve active credentials.",
                code="DB_CREDENTIALS_RESOLVE_FAILED",
                context={
                    "component": self.name,
                    "context_id": self.context_id,
                    "environment": env_str,
                },
            ) from exc

    @model_validator(mode="after")
    def _build_objects(self) -> "DatabaseComponent":
        # Resolve once at model build so config errors fail early
        self._resolve_credentials_for_environment()
        return self

    def prepare_for_execution(self, environment: Optional[str] = None) -> None:
        self._resolve_credentials_for_environment(environment)

    def _get_credentials(self) -> Dict[str, Any]:
        """
        Provide a stable mapping for receivers, using the already resolved creds.
        """
        if self._credentials is None:
            ctx = self.get_resolved_context()
            assert isinstance(ctx, CredentialsMappingContext)
            self._credentials, self._cred_id = ctx.resolve_active_credentials()

        creds = self._credentials
        return {
            "user": creds.get_parameter("user"),
            "password": creds.decrypted_password,
            "database": creds.get_parameter("database"),
            "host": creds.get_parameter("host"),
            "port": creds.get_parameter("port"),
            "pool_max_size": creds.get_parameter("pool_max_size"),
            "pool_timeout_s": creds.get_parameter("pool_timeout_s"),
            "__credentials_id__": self._cred_id,
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
