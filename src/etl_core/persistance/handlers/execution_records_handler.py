from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Optional

from sqlmodel import Session

from etl_core.persistance.db import engine
from etl_core.persistance.table_definitions import (
    ExecutionAttemptTable,
    ExecutionTable,
)


class ExecutionRecordsHandler:
    """
    Thin persistence layer for executions & attempts.
    """

    def __init__(self, engine_=engine) -> None:
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
