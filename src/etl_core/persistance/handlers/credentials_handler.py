from __future__ import annotations

import os
from typing import Optional, Tuple, List

from sqlmodel import Session, select

from etl_core.persistance.db import engine
from etl_core.persistance.table_definitions import CredentialsTable
from etl_core.context.credentials import Credentials
from etl_core.context.secrets.keyring_provider import KeyringSecretProvider
from etl_core.context.secrets.memory_provider import InMemorySecretProvider
from etl_core.context.secrets.secret_provider import SecretProvider

_MEMORY_PROVIDER_SINGLETON: Optional[InMemorySecretProvider] = None


def _make_secret_provider() -> SecretProvider:
    """
    Decide the secret backend at runtime.
      - SECRET_BACKEND: "memory" (default) or "keyring"
    """
    backend = os.getenv("SECRET_BACKEND", "memory").strip().lower()
    if backend == "memory":
        global _MEMORY_PROVIDER_SINGLETON
        if _MEMORY_PROVIDER_SINGLETON is None:
            _MEMORY_PROVIDER_SINGLETON = InMemorySecretProvider()
        return _MEMORY_PROVIDER_SINGLETON
    if backend == "keyring":
        return KeyringSecretProvider(service="etl_core")
    raise ValueError(f"Unsupported SECRET_BACKEND={backend!r}")


class CredentialsHandler:
    """
    CRUD for credentials metadata; secrets are stored/retrieved via secret backend.
    Single identifier model: use the same UUID string everywhere (Credentials.id).
    """

    def __init__(self, engine_=engine) -> None:
        self.engine = engine_
        self.secret_store = _make_secret_provider()

    def _password_key(self, credentials_id: str) -> str:
        return f"{credentials_id}/password"

    def upsert(self, creds: Credentials) -> str:
        """
        Insert or update non-secret metadata; store password in secret backend.
        Returns the credentials_id.
        """
        with Session(self.engine) as s:
            row = s.exec(
                select(CredentialsTable).where(
                    CredentialsTable.id == creds.credentials_id
                )
            ).first()
            if row is None:
                row = CredentialsTable(id=creds.credentials_id)

            row.name = creds.name
            row.user = creds.user
            row.host = creds.host
            row.port = creds.port
            row.database = creds.database
            row.pool_max_size = creds.pool_max_size
            row.pool_timeout_s = creds.pool_timeout_s

            s.add(row)
            s.commit()
            s.refresh(row)

        if creds.decrypted_password:
            self.secret_store.set(
                self._password_key(creds.credentials_id), creds.decrypted_password
            )

        return row.id

    def get_by_id(self, credentials_id: str) -> Optional[Tuple[Credentials, str]]:
        """
        Returns (hydrated Credentials, credentials_id) or None.
        Hydration pulls password from secret backend into the domain model.
        """
        with Session(self.engine) as s:
            row = s.exec(
                select(CredentialsTable).where(CredentialsTable.id == credentials_id)
            ).first()
            if row is None:
                return None

        try:
            password = self.secret_store.get(self._password_key(row.id))
        except KeyError:
            password = None

        model = Credentials(
            credentials_id=row.id,
            name=row.name,
            user=row.user,
            host=row.host,
            port=row.port,
            database=row.database,
            password=password,
            pool_max_size=row.pool_max_size,
            pool_timeout_s=row.pool_timeout_s,
        )
        return model, row.id

    def list_all(self) -> List[CredentialsTable]:
        with Session(self.engine) as s:
            return list(s.exec(select(CredentialsTable)).all())

    def delete_by_id(self, credentials_id: str) -> None:
        with Session(self.engine) as s:
            row = s.exec(
                select(CredentialsTable).where(CredentialsTable.id == credentials_id)
            ).first()
            if row:
                s.delete(row)
                s.commit()
        self.secret_store.delete(self._password_key(credentials_id))
