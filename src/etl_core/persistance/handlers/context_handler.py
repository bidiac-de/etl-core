from __future__ import annotations

from contextlib import contextmanager
from typing import Dict, Iterable, Iterator, List, Optional

from sqlmodel import Session, select

from etl_core.persistance.db import engine
from etl_core.persistance.table_definitions import (
    ContextParameterTable,
    ContextTable,
)


class ContextHandler:
    """
    Persistence for contexts:
      - Stores non-secret metadata (name, environment) in ContextTable.
      - Tracks parameter presence in ContextParameterTable.
      - Secret values remain in your keyring under provider_id/<key>.
    """

    def __init__(self) -> None:
        self.engine = engine

    @contextmanager
    def _session(self) -> Iterator[Session]:
        with Session(self.engine) as s:
            yield s

    def upsert(
        self,
        *,
        provider_id: str,
        name: str,
        environment: str,
        non_secure_params: Dict[str, str],
        secure_param_keys: Iterable[str],
    ) -> ContextTable:
        """
        Idempotently writes a context row and replaces all its parameter rows.

        - DB stores only non-secure param values.
        - Secure params are represented with is_secure=True and empty value.
        """
        secure_keys = set(secure_param_keys)
        with self._session() as s:
            row = s.exec(
                select(ContextTable).where(ContextTable.provider_id == provider_id)
            ).first()
            if row is None:
                row = ContextTable(
                    provider_id=provider_id,
                    name=name,
                    environment=environment,
                )
            else:
                row.name = name
                row.environment = environment

            s.add(row)
            s.flush()  # ensure row is persisted before parameter ops

            # Replace parameters in one go for simplicity and correctness
            existing: List[ContextParameterTable] = s.exec(
                select(ContextParameterTable).where(
                    ContextParameterTable.context_provider_id == provider_id
                )
            ).all()
            for e in existing:
                s.delete(e)

            # Non-secure: store key and value
            for k, v in non_secure_params.items():
                s.add(
                    ContextParameterTable(
                        context_provider_id=provider_id,
                        key=k,
                        value=str(v),
                        is_secure=False,
                    )
                )

            # Secure: store key only (value lives in keyring)
            for k in secure_keys:
                s.add(
                    ContextParameterTable(
                        context_provider_id=provider_id,
                        key=k,
                        value="",
                        is_secure=True,
                    )
                )

            s.commit()
            s.refresh(row)
            return row

    def list_all(self) -> List[ContextTable]:
        """Return all persisted contexts (no secrets)."""
        with self._session() as s:
            return list(s.exec(select(ContextTable)).all())

    def get_by_provider_id(self, provider_id: str) -> Optional[ContextTable]:
        """Return a single context by provider_id, or None."""
        with self._session() as s:
            return s.exec(
                select(ContextTable).where(ContextTable.provider_id == provider_id)
            ).first()

    def delete_by_provider_id(self, provider_id: str) -> None:
        """
        Delete a context row and all its parameter rows.
        (Secrets should be removed separately from keyring by the caller.)
        """
        with self._session() as s:
            # Remove parameters first
            params = s.exec(
                select(ContextParameterTable).where(
                    ContextParameterTable.context_provider_id == provider_id
                )
            ).all()
            for p in params:
                s.delete(p)

            # Remove the context row
            row = s.exec(
                select(ContextTable).where(ContextTable.provider_id == provider_id)
            ).first()
            if row is not None:
                s.delete(row)

            s.commit()
