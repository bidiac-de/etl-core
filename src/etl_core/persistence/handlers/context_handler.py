from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple

from sqlmodel import select
from sqlalchemy.exc import IntegrityError

from etl_core.persistence.handlers.base_handler import BaseHandler
from etl_core.persistence.table_definitions import (
    ContextParameterTable,
    ContextTable,
    ContextCredentialsMapTable,
)
from etl_core.context.credentials_mapping_context import CredentialsMappingContext


class ContextHandler(BaseHandler):
    """
    Persistence for contexts:
      - Stores non-secret metadata (name, environment) in ContextTable.
      - Tracks parameter presence in ContextParameterTable.
      - Secret values remain in secret backend keyed by context id / <key>.
      - Stores env->credentials_id rows in ContextCredentialsMapTable for
        CredentialsMappingContext.
    """

    _table = ContextTable

    def upsert(
        self,
        *,
        context_id: str,
        name: str,
        environment: str,
        non_secure_params: Dict[str, str],
        secure_param_keys: Iterable[str],
    ) -> ContextTable:
        """
        Idempotently writes a context row and replaces all its parameter rows.
        DB stores only non-secure param values; secure params are placeholder rows.
        """
        secure_keys = set(secure_param_keys)
        with self._session() as s:
            row = s.exec(
                select(ContextTable).where(ContextTable.id == context_id)
            ).first()
            if row is None:
                row = ContextTable(id=context_id, name=name, environment=environment)
            else:
                row.name = name
                row.environment = environment

            s.add(row)
            s.flush()

            existing: List[ContextParameterTable] = s.exec(
                select(ContextParameterTable).where(
                    ContextParameterTable.context_id == context_id
                )
            ).all()
            for e in existing:
                s.delete(e)

            # Non-secure: store key and value
            for k, v in non_secure_params.items():
                s.add(
                    ContextParameterTable(
                        context_id=context_id,
                        key=k,
                        value=str(v),
                        is_secure=False,
                    )
                )

            # Secure: store key only (value lives in keyring)
            for k in secure_keys:
                s.add(
                    ContextParameterTable(
                        context_id=context_id,
                        key=k,
                        value="",
                        is_secure=True,
                    )
                )

            s.commit()
            s.refresh(row)
            return row

    def upsert_credentials_mapping_context(
        self,
        *,
        context_id: str,
        name: str,
        environment: str,
        mapping_env_to_credentials_id: Dict[str, str],
    ) -> ContextTable:
        """
        Create/update a context and replace its env->credentials_id mapping.
        `mapping_env_to_credentials_id` uses raw environment values (e.g. 'TEST').
        """
        with self._session() as s:
            row = s.exec(
                select(ContextTable).where(ContextTable.id == context_id)
            ).first()
            if row is None:
                row = ContextTable(id=context_id, name=name, environment=environment)
            else:
                row.name = name
                row.environment = environment

            s.add(row)
            s.flush()

            existing = s.exec(
                select(ContextCredentialsMapTable).where(
                    ContextCredentialsMapTable.context_id == context_id
                )
            ).all()
            for e in existing:
                s.delete(e)

            for env_value, cred_id in mapping_env_to_credentials_id.items():
                s.add(
                    ContextCredentialsMapTable(
                        context_id=context_id,
                        environment=env_value,
                        credentials_id=cred_id,
                    )
                )

            s.commit()
            s.refresh(row)
            return row

    def get_credentials_map(self, context_id: str) -> Dict[str, str]:
        """
        Return environment -> credentials_id mapping for a context.
        """
        with self._session() as s:
            rows = s.exec(
                select(ContextCredentialsMapTable).where(
                    ContextCredentialsMapTable.context_id == context_id
                )
            ).all()
            return {r.environment: r.credentials_id for r in rows}

    def list_all(self) -> List[ContextTable]:
        """Return all persisted contexts (no secrets)."""
        return self._list_all()

    def get_by_id(
        self, context_id: str
    ) -> Optional[Tuple[CredentialsMappingContext, str]]:
        """
        Return a hydrated CredentialsMappingContext and its id.
        The mapping uses raw env values (e.g. "TEST") mapped to credentials ids.
        """
        with self._session() as s:
            row = s.exec(
                select(ContextTable).where(ContextTable.id == context_id)
            ).first()
            if row is None:
                return None

        env_to_creds = self.get_credentials_map(context_id)

        ctx = CredentialsMappingContext(
            id=context_id,
            name=row.name,
            environment=row.environment,
            credentials_ids=env_to_creds,
        )
        return ctx, context_id

    def delete_by_id(self, context_id: str) -> bool:
        """
        Delete a context row, its parameter rows, and its mapping rows.
        Returns True if a Context row was deleted, False if it didn't exist.
        Raises IntegrityError if the delete is blocked by FK constraints.
        (Secrets should be removed separately by the caller.)
        """
        with self._session() as s:
            params = s.exec(
                select(ContextParameterTable).where(
                    ContextParameterTable.context_id == context_id
                )
            ).all()
            for p in params:
                s.delete(p)

            maps = s.exec(
                select(ContextCredentialsMapTable).where(
                    ContextCredentialsMapTable.context_id == context_id
                )
            ).all()
            for m in maps:
                s.delete(m)

            row = s.exec(
                select(ContextTable).where(ContextTable.id == context_id)
            ).first()

            if row is None:
                s.commit()
                return False

            try:
                s.delete(row)
                s.commit()
            except IntegrityError:
                s.rollback()
                raise

            return True
