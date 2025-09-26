from __future__ import annotations

import logging
from typing import Optional, Tuple, List

from sqlmodel import Session, select
from sqlalchemy.exc import IntegrityError

from etl_core.persistence.db import engine, ensure_schema
from etl_core.persistence.table_definitions import CredentialsTable
from etl_core.context.credentials import Credentials
from etl_core.context.secrets.secret_utils import create_secret_provider


def _mask_secret(secret: Optional[str]) -> str:
    if not secret:
        return "<unset>"

    if len(secret) == 1:
        return f"{secret}***"

    head = secret[0]
    tail = secret[-1]
    return f"{head}***{tail}"


class CredentialsHandler:
    """
    CRUD for credentials metadata; secrets are stored/retrieved via secret backend.
    Identifier model: system generates and returns the UUID string.
    """

    def __init__(self, engine_=engine) -> None:
        ensure_schema()
        self.engine = engine_
        self.secret_store = create_secret_provider()
        self._log = logging.getLogger("etl_core.persistence.credentials")

    def _password_key(self, credentials_id: str) -> str:
        return f"{credentials_id}/password"

    def upsert(
        self, creds: Credentials, *, credentials_id: Optional[str] = None
    ) -> str:
        """
        Insert or update non-secret metadata; store password in secret backend.
        Returns the "credentials_id" assigned by the system.
        If "credentials_id" is provided, upsert that row;
         otherwise a new row is created.
        """
        with Session(self.engine) as s:
            existing_row: Optional[CredentialsTable] = None
            if credentials_id:
                existing_row = s.exec(
                    select(CredentialsTable).where(
                        CredentialsTable.id == credentials_id
                    )
                ).first()

            is_update = existing_row is not None
            row = existing_row or (
                CredentialsTable(id=credentials_id)
                if credentials_id
                else CredentialsTable()
            )

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

        # Persist secret after we know the final id
        if creds.decrypted_password:
            self.secret_store.set(self._password_key(row.id), creds.decrypted_password)

        masked_password = _mask_secret(creds.decrypted_password)
        action = "updated" if is_update else "created"
        self._log.info(
            "Credentials %s %s (user=%s, host=%s, port=%s, password=%s)",
            row.id,
            action,
            row.user,
            row.host,
            row.port,
            masked_password,
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
            name=row.name,
            user=row.user,
            host=row.host,
            port=row.port,
            database=row.database,
            password=password,
            pool_max_size=row.pool_max_size,
            pool_timeout_s=row.pool_timeout_s,
        )
        self._log.info(
            "Credentials %s accessed (user=%s, host=%s, port=%s, password=%s)",
            row.id,
            row.user,
            row.host,
            row.port,
            _mask_secret(password),
        )
        return model, row.id

    def list_all(self) -> List[CredentialsTable]:
        with Session(self.engine) as s:
            return list(s.exec(select(CredentialsTable)).all())

    def delete_by_id(self, credentials_id: str) -> bool:
        """
        Delete a credentials row and its secret.
        Returns True if the row existed and was deleted, False otherwise.
        Raises IntegrityError if the delete is blocked by FK constraints.
        """
        deleted = False
        with Session(self.engine) as s:
            row = s.exec(
                select(CredentialsTable).where(CredentialsTable.id == credentials_id)
            ).first()
            if row:
                try:
                    s.delete(row)
                    s.commit()
                    deleted = True
                except IntegrityError:
                    s.rollback()
                    raise

        # Secrets: best-effort cleanup — don't fail if the secret didn't exist
        try:
            self.secret_store.delete(self._password_key(credentials_id))
        except Exception:
            # Ignore missing/other secret backend issues on delete
            pass

        return deleted
