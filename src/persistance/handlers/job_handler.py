from __future__ import annotations

from typing import List, Optional
from contextlib import contextmanager

from sqlmodel import Session, select
from src.persistance.db import engine
from src.job_execution.runtimejob import RuntimeJob
from src.persistance.table_definitions import JobTable
from src.persistance.handlers.dataclasses_handler import DataClassHandler
from src.persistance.handlers.components_handler import ComponentHandler


class JobHandler:
    """
    CRUD for JobTable with clean session handling and simple component updates.
    """

    def __init__(self, engine_=engine):
        self.engine = engine_
        self.dc = DataClassHandler()
        self.ch = ComponentHandler(self.dc)

    @contextmanager
    def _session(self) -> Session:
        with Session(self.engine) as session:
            yield session

    def create_job_entry(self, job: RuntimeJob) -> JobTable:
        with self._session() as session:
            job_meta = self.dc.create_metadata_entry(
                session, job.metadata_.model_dump()
            )

            row = JobTable(
                id=job.id,
                name=job.name,
                num_of_retries=job.num_of_retries,
                file_logging=job.file_logging,
                strategy_type=job.strategy_type,
                metadata_=job_meta,
            )
            session.add(row)
            session.flush()

            self.ch.create_all(session, row, job.components)

            session.commit()
            session.refresh(row)
            return row

    def update(self, job: RuntimeJob) -> JobTable:
        with self._session() as session:
            row = session.get(JobTable, job.id)
            if row is None:
                raise ValueError(f"Job with id {job.id!r} not found")

            # simple field updates
            row.name = job.name
            row.num_of_retries = job.num_of_retries
            row.file_logging = job.file_logging
            row.strategy_type = job.strategy_type

            # metadata
            if row.metadata_ is None:
                row.metadata_ = self.dc.create_metadata_entry(
                    session, job.metadata_.model_dump()
                )
            else:
                self.dc.update_metadata_entry(
                    session, row.metadata_, job.metadata_.model_dump()
                )

            # replace components
            self.ch.replace_all(session, row, job.components)

            session.add(row)
            session.commit()
            session.refresh(row)
            return row

    def delete(self, job_id: str) -> None:
        with self._session() as session:
            row = session.get(JobTable, job_id)
            if row is None:
                raise ValueError(f"Job with id {job_id!r} not found")

            # components + links + dataclasses
            self.ch.delete_all(session, row)
            if row.metadata_ is not None:
                session.delete(row.metadata_)

            session.delete(row)
            session.commit()

    def get_by_id(self, job_id: str) -> Optional[JobTable]:
        with self._session() as session:
            return session.get(JobTable, job_id)

    def get_all(self) -> List[JobTable]:
        with self._session() as session:
            # materialize into a list for strict typing
            return list(session.exec(select(JobTable)).all())

    def record_to_job(self, record: JobTable) -> RuntimeJob:
        """
        build a RuntimeJob from a JobTable record.
        """
        # fresh relationships for components; load inside a session.
        with self._session() as session:
            rec = session.get(JobTable, record.id)
            if rec is None:
                raise ValueError(f"Job with id {record.id!r} not found")

            comps = self.ch.hydrate_all(rec)

            payload = {
                "name": rec.name,
                "num_of_retries": rec.num_of_retries,
                "file_logging": rec.file_logging,
                "strategy_type": rec.strategy_type,
                "components": [c.model_dump() for c in comps],
                "metadata": rec.metadata_.model_dump() if rec.metadata else {},
            }
            job = RuntimeJob(**payload)
            # Keep persisted id
            object.__setattr__(job, "_id", rec.id)
            return job
