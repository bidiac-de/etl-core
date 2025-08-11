from __future__ import annotations

from contextlib import contextmanager
from typing import List, Optional
from uuid import uuid4

from sqlalchemy.orm import selectinload
from sqlmodel import Session, select

from src.job_execution.runtimejob import RuntimeJob
from src.persistance.configs.job_config import JobConfig
from src.persistance.db import engine
from src.persistance.handlers.components_handler import ComponentHandler
from src.persistance.handlers.dataclasses_handler import DataClassHandler
from src.persistance.table_definitions import ComponentTable, JobTable


class JobHandler:
    """
    CRUD for JobTable with clean session handling.
    Optimizations:
      - Avoid unnecessary flush/refresh calls.
      - Eager-load relationships for hydration to prevent N+1 queries.
      - Use no_autoflush around multi-step deletes/updates that would
        otherwise trigger premature flushes.
    """

    def __init__(self, engine_=engine) -> None:
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
        Keep a single commit and avoid intermediate refreshes.
        """
        with self._session() as session:
            # Prepare parent job row and metadata first
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

            # Child rows need job id
            self.ch.create_all_from_configs(session, row, cfg.components)

            session.commit()
            # Refresh once after commit to return a managed instance with ids
            session.refresh(row)
            return row

    def update(self, job_id: str, cfg: JobConfig) -> JobTable:
        """
        Replace an existing job with data from a pure config.
        Eager-loading is not required here; we delete and reinsert components.
        """
        with self._session() as session:
            row = session.get(JobTable, job_id)
            if row is None:
                raise ValueError(f"Job with id {job_id!r} not found")

            # Simple fields
            row.name = cfg.name
            row.num_of_retries = cfg.num_of_retries
            row.file_logging = cfg.file_logging
            row.strategy_type = cfg.strategy_type

            # Metadata (create or update)
            if row.metadata_ is None:
                row.metadata_ = self.dc.create_metadata_entry(
                    session, cfg.metadata_.model_dump()
                )
            else:
                self.dc.update_metadata_entry(
                    session, row.metadata_, cfg.metadata_.model_dump()
                )

            # Replace all components in one go
            # Use no_autoflush to avoid accidental flushes when
            with session.no_autoflush:
                self.ch.replace_all_from_configs(session, row, cfg.components)

            session.commit()
            session.refresh(row)
            return row

    def delete(self, job_id: str) -> None:
        """
        Delete a job and all related rows (components, links, layout, metadata).
        """
        with self._session() as session:
            row = session.get(JobTable, job_id)
            if row is None:
                raise ValueError(f"Job with id {job_id!r} not found")

            # Delete components/links/child rows
            with session.no_autoflush:
                self.ch.delete_all(session, row)

                # Capture job metadata while parent row is still present
                job_meta = row.metadata_

                # Delete parent job, then metadata
                session.delete(row)
                if job_meta is not None:
                    session.delete(job_meta)

            session.commit()

    def get_by_id(self, job_id: str) -> Optional[JobTable]:
        with self._session() as session:
            return session.get(JobTable, job_id)

    def get_all(self) -> List[JobTable]:
        """
        Return all job rows. Callers can decide how much to hydrate.
        """
        with self._session() as session:
            return list(session.exec(select(JobTable)).all())

    def record_to_job(self, record: JobTable) -> RuntimeJob:
        """
        Build a RuntimeJob from a JobTable record with eager-loaded relationships
        to avoid N+1 round-trips.
        """
        with self._session() as session:
            # Eager-load components, their layout/metadata, and next-links
            stmt = (
                select(JobTable)
                .where(JobTable.id == record.id)
                .options(
                    selectinload(JobTable.metadata_),
                    selectinload(JobTable.components).selectinload(
                        ComponentTable.layout
                    ),
                    selectinload(JobTable.components).selectinload(
                        ComponentTable.metadata_
                    ),
                    selectinload(JobTable.components).selectinload(
                        ComponentTable.next_components
                    ),
                )
            )
            rec = session.exec(stmt).first()
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
