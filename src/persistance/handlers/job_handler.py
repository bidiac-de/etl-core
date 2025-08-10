from __future__ import annotations
from uuid import uuid4

from typing import List, Optional
from contextlib import contextmanager

from sqlmodel import Session, select
from src.persistance.db import engine
from src.job_execution.runtimejob import RuntimeJob
from src.persistance.configs.job_config import JobConfig
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

    def create_job_entry(self, cfg: JobConfig) -> JobTable:
        """
        Persist a pure config. DB/system generates IDs.
        """
        with self._session() as session:
            job_meta = self.dc.create_metadata_entry(
                session, cfg.metadata_.model_dump()
            )
            row = JobTable(
                id=str(uuid4()),
                name=cfg.name,
                num_of_retries=cfg.num_of_retries,
                file_logging=cfg.file_logging,
                strategy_type=cfg.strategy_type,
                metadata_=job_meta,
            )
            session.add(row)
            session.flush()

            self.ch.create_all_from_configs(session, row, cfg.components)
            session.commit()
            session.refresh(row)
            return row

    def update(self, job_id: str, cfg: JobConfig) -> JobTable:
        """
        Replace an existing job with data from a pure config.
        """
        with self._session() as session:
            row = session.get(JobTable, job_id)
            if row is None:
                raise ValueError(f"Job with id {job_id!r} not found")

            # simple fields
            row.name = cfg.name
            row.num_of_retries = cfg.num_of_retries
            row.file_logging = cfg.file_logging
            row.strategy_type = cfg.strategy_type

            # metadata
            if row.metadata_ is None:
                row.metadata_ = self.dc.create_metadata_entry(
                    session, cfg.metadata_.model_dump()
                )
            else:
                self.dc.update_metadata_entry(
                    session, row.metadata_, cfg.metadata_.model_dump()
                )

            self.ch.replace_all_from_configs(session, row, cfg.components)
            session.add(row)
            session.commit()
            session.refresh(row)
            return row

    def delete(self, job_id: str) -> None:
        with self._session() as session:
            row = session.get(JobTable, job_id)
            if row is None:
                raise ValueError(f"Job with id {job_id!r} not found")

            # Delete all components, links, metadata
            self.ch.delete_all(session, row)

            # Capture metadata row while job is still loaded
            job_meta = row.metadata_

            # Critical: prevent autoflush that would null job.metadata_id
            with session.no_autoflush:
                # Delete parent job first so the FK no longer exists
                session.delete(row)
                # safe to delete the metadata row now
                if job_meta is not None:
                    session.delete(job_meta)

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
        # fresh relationships for components; load inside a session
        with self._session() as session:
            rec = session.get(JobTable, record.id)
            if rec is None:
                raise ValueError(f"Job with id {record.id!r} not found")

            payload = {
                "name": rec.name,
                "num_of_retries": rec.num_of_retries,
                "file_logging": rec.file_logging,
                "strategy_type": rec.strategy_type,
                "components": [],
                "metadata": {},
            }
            job = RuntimeJob(**payload)
            # Keep persisted id
            object.__setattr__(job, "_id", rec.id)

            comps = self.ch.build_runtime_for_all(rec)
            job.components = comps
            if rec.metadata_ is not None:
                job.metadata_ = self.dc.dump_metadata(rec.metadata_)
            return job
