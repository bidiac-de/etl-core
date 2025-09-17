from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Optional, Iterable

from sqlmodel import Session, select, asc, desc

from etl_core.persistence.db import engine, ensure_schema
from etl_core.persistence.table_definitions import (
    ExecutionAttemptTable,
    ExecutionTable,
)


class ExecutionRecordsHandler:
    """
    Thin persistence layer for executions & attempts.
    """

    def __init__(self, engine_=engine) -> None:
        ensure_schema()
        self.engine = engine_

    @contextmanager
    def _session(self) -> Session:
        with Session(self.engine) as session:
            yield session

    def create_execution(
        self,
        execution_id: str,
        job_id: str,
        environment: Optional[str],
    ) -> None:
        with self._session() as session:
            row = ExecutionTable(
                id=execution_id,
                job_id=job_id,
                environment=environment,
                status="RUNNING",
                started_at=datetime.now(),
            )
            session.add(row)
            session.commit()

    def finalize_execution(
        self,
        execution_id: str,
        status: str,
        error: Optional[str] = None,
    ) -> None:
        with self._session() as session:
            row = session.get(ExecutionTable, execution_id)
            if row is None:
                return
            row.status = status
            row.error = error
            row.finished_at = datetime.now()
            session.add(row)
            session.commit()

    def start_attempt(
        self,
        attempt_id: str,
        execution_id: str,
        attempt_index: int,
    ) -> None:
        with self._session() as session:
            row = ExecutionAttemptTable(
                id=attempt_id,
                execution_id=execution_id,
                attempt_index=attempt_index,
                status="RUNNING",
                started_at=datetime.now(),
            )
            session.add(row)
            session.commit()

    def finish_attempt(
        self,
        attempt_id: str,
        status: str,
        error: Optional[str] = None,
    ) -> None:
        with self._session() as session:
            row = session.get(ExecutionAttemptTable, attempt_id)
            if row is None:
                return
            row.status = status
            row.error = error
            row.finished_at = datetime.now()
            session.add(row)
            session.commit()

    def list_executions(
        self,
        *,
        job_id: Optional[str] = None,
        status: Optional[str] = None,
        environment: Optional[str] = None,
        started_after: Optional[datetime] = None,
        started_before: Optional[datetime] = None,
        sort_by: str = "started_at",
        order: str = "desc",
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[ExecutionTable], int]:
        """
        Returns (rows, total_count) with filtering + pagination.
        Keeps logic small and predictable; no ORM magic outside handler.
        """
        valid_sort = {"started_at", "finished_at", "status"}
        sort_col = sort_by if sort_by in valid_sort else "started_at"
        sort_dir = desc if order.lower() == "desc" else asc

        with self._session() as session:
            stmt = select(ExecutionTable)
            if job_id:
                stmt = stmt.where(ExecutionTable.job_id == job_id)
            if status:
                stmt = stmt.where(ExecutionTable.status == status)
            if environment:
                stmt = stmt.where(ExecutionTable.environment == environment)
            if started_after:
                stmt = stmt.where(ExecutionTable.started_at >= started_after)
            if started_before:
                stmt = stmt.where(ExecutionTable.started_at <= started_before)

            stmt = stmt.order_by(sort_dir(getattr(ExecutionTable, sort_col)))

            total = session.exec(
                stmt.with_only_columns(ExecutionTable.id)  # type: ignore[arg-type]
            ).all()
            paged = session.exec(stmt.offset(offset).limit(limit)).all()

        return list(paged), len(total)

    def get_execution(
        self, execution_id: str
    ) -> tuple[Optional[ExecutionTable], list[ExecutionAttemptTable]]:
        with self._session() as session:
            exec_row = session.get(ExecutionTable, execution_id)
            if exec_row is None:
                return None, []
            attempts = session.exec(
                select(ExecutionAttemptTable)
                .where(ExecutionAttemptTable.execution_id == execution_id)
                .order_by(asc(ExecutionAttemptTable.attempt_index))
            ).all()
            return exec_row, list(attempts)

    def list_attempts(self, execution_id: str) -> list[ExecutionAttemptTable]:
        with self._session() as session:
            rows: Iterable[ExecutionAttemptTable] = session.exec(
                select(ExecutionAttemptTable)
                .where(ExecutionAttemptTable.execution_id == execution_id)
                .order_by(asc(ExecutionAttemptTable.attempt_index))
            )
            return list(rows)
