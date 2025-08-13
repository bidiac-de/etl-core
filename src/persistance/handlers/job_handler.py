from __future__ import annotations

from contextlib import contextmanager
from typing import List, Optional, Dict, Any
from uuid import uuid4

from sqlalchemy.orm import selectinload
from sqlmodel import Session, select

from src.job_execution.runtimejob import RuntimeJob
from src.persistance.configs.job_config import JobConfig
from src.persistance.db import engine
from src.persistance.handlers.components_handler import ComponentHandler
from src.persistance.handlers.dataclasses_handler import DataClassHandler
from src.persistance.table_definitions import (
    JobTable,
    ComponentTable,
    ComponentNextLink,
)


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

    def get_by_id(self, job_id: str) -> Optional[JobTable]:
        with self._session() as session:
            return session.get(JobTable, job_id)

    def get_all(self) -> List[JobTable]:
        """
        Return all job rows. Callers can decide how much to hydrate.
        """
        with self._session() as session:
            return list(session.exec(select(JobTable)).all())

    def _build_runtime_from_record(self, rec: JobTable) -> RuntimeJob:
        """
        Build a RuntimeJob from an already-loaded JobTable record WITHOUT any
        additional queries or re-fetching. Single responsibility; low complexity.
        """
        job = RuntimeJob(
            name=rec.name,
            num_of_retries=rec.num_of_retries,
            file_logging=rec.file_logging,
            strategy_type=rec.strategy_type,
            components=[],
            metadata={},
        )
        # Keep persisted id (private attribute on RuntimeJob)
        object.__setattr__(job, "_id", rec.id)

        # Components + metadata from preloaded relationships
        comps = self.ch.build_runtime_for_all(rec)
        job.components = comps
        if rec.metadata_ is not None:
            job.metadata_ = self.dc.dump_metadata(rec.metadata_)

        return job

    def load_runtime_job(self, job_id: str) -> RuntimeJob:
        """
        One-shot loader: fetch + hydrate a single job with all relationships,
        no preflight/get-by-id round-trips.
        """
        with self._session() as session:
            stmt = (
                select(JobTable)
                .where(JobTable.id == job_id)
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
                raise ValueError(f"Job with id {job_id!r} not found")
            return self._build_runtime_from_record(rec)

    def list_jobs_brief(self) -> List[Dict[str, Any]]:
        """
        Load all jobs once (eagerly) and return lightweight summaries
        WITHOUT re-fetching per row. No TOCTOU double-lookups.
        """
        with self._session() as session:
            stmt = select(JobTable).options(
                selectinload(JobTable.metadata_),
                # for consistent component counting/hydration later if needed:
                selectinload(JobTable.components).selectinload(ComponentTable.layout),
                selectinload(JobTable.components).selectinload(
                    ComponentTable.metadata_
                ),
                selectinload(JobTable.components).selectinload(
                    ComponentTable.next_components
                ),
            )
            records = session.exec(stmt).all()

            result: List[Dict[str, Any]] = []
            for rec in records:
                job = self._build_runtime_from_record(rec)
                # Return a brief object (omit heavy components list)
                data = job.model_dump(exclude={"components"})
                data["id"] = job.id
                result.append(data)
            return result

    def delete(self, job_id: str) -> None:
        """
        Delete a job and all related rows in one transaction.
        Explicitly removes:
          - component next-link rows
          - component layouts
          - component metadata
          - components
          - job metadata
          - job
        """
        with self._session() as session:
            # Load the job with everything we need to avoid N+1.
            job = session.exec(
                select(JobTable)
                .where(JobTable.id == job_id)
                .options(
                    selectinload(JobTable.components).selectinload(
                        ComponentTable.layout
                    ),
                    selectinload(JobTable.components).selectinload(
                        ComponentTable.metadata_
                    ),
                    selectinload(JobTable.components),
                    selectinload(JobTable.metadata_),
                )
            ).first()
            if job is None:
                raise ValueError(f"Job with id {job_id!r} not found")

            comps: List[ComponentTable] = list(job.components)
            comp_ids = [c.id for c in comps]

            # Remove next-link rows that reference any of this job's components
            if comp_ids:
                links = list(
                    session.exec(
                        select(ComponentNextLink).where(
                            (ComponentNextLink.component_id.in_(comp_ids))
                            | (ComponentNextLink.next_id.in_(comp_ids))
                        )
                    )
                )
                for link in links:
                    session.delete(link)

            # Remove component-scoped dependents, then components themselves.
            for c in comps:
                if c.layout is not None:
                    session.delete(c.layout)
                if c.metadata_ is not None:
                    session.delete(c.metadata_)
                session.delete(c)

            # Finally remove job-scoped metadata and the job row.
            if job.metadata_ is not None:
                session.delete(job.metadata_)
            session.delete(job)

            session.commit()

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
