from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, Type, TypeVar, Optional, List

from sqlmodel import Session, SQLModel, select

from etl_core.persistence.db import engine, ensure_schema

T = TypeVar("T", bound=SQLModel)


class BaseHandler:
    """
    Abstract base class for persistence handlers.

    Provides:
      - Common engine initialization with schema guarantee
      - Reusable session context manager
      - Generic CRUD helpers for simple table operations
    """

    # Subclasses can override to specify their primary table type
    _table: Type[SQLModel] | None = None

    def __init__(self, engine_=engine) -> None:
        ensure_schema()
        self.engine = engine_

    @contextmanager
    def _session(self) -> Iterator[Session]:
        """Yield a SQLModel Session scoped to one transaction."""
        with Session(self.engine) as session:
            yield session

    # --- Generic CRUD helpers (use when _table is set or pass table_cls) ---

    def _get_by_id(
        self, id_: str, table_cls: Type[T] | None = None
    ) -> Optional[T]:
        """
        Fetch a single row by primary key.
        Uses `table_cls` if provided, otherwise falls back to `self._table`.
        """
        tbl = table_cls or self._table
        if tbl is None:
            raise NotImplementedError("No table class specified")
        with self._session() as s:
            return s.get(tbl, id_)

    def _list_all(self, table_cls: Type[T] | None = None) -> List[T]:
        """
        Return all rows from the specified table.
        Uses `table_cls` if provided, otherwise falls back to `self._table`.
        """
        tbl = table_cls or self._table
        if tbl is None:
            raise NotImplementedError("No table class specified")
        with self._session() as s:
            return list(s.exec(select(tbl)).all())

    def _delete_by_id(
        self,
        id_: str,
        table_cls: Type[T] | None = None,
        *,
        not_found_error: Type[Exception] | None = None,
    ) -> bool:
        """
        Delete a row by primary key. Returns True if deleted, False if not found.
        If `not_found_error` is provided, raises that exception when row is missing.
        """
        tbl = table_cls or self._table
        if tbl is None:
            raise NotImplementedError("No table class specified")
        with self._session() as s:
            row = s.get(tbl, id_)
            if row is None:
                if not_found_error:
                    raise not_found_error(id_)
                return False
            s.delete(row)
            s.commit()
            return True


