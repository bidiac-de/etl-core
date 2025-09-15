from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, List, Optional
from uuid import uuid4

from sqlalchemy.orm import selectinload
from sqlmodel import Session, select

from etl_core.job_execution.runtimejob import RuntimeJob
from etl_core.persistence.configs.job_config import JobConfig
from etl_core.persistence.db import engine
from etl_core.persistence.errors import PersistNotFoundError
from etl_core.persistence.handlers.components_handler import ComponentHandler
from etl_core.persistence.handlers.dataclasses_handler import DataClassHandler
from etl_core.persistence.table_definitions import (
    ComponentTable,
    JobTable,
    ComponentLinkTable,
)


class JobHandler:
    def __init__(self, engine_=engine) -> None:
        self.engine = engine_
        self.dc = DataClassHandler()
        self.ch = ComponentHandler(self.dc)

    @contextmanager
    def _session(self) -> Session:
        with Session(self.engine) as session:
            yield session

    def create_job_entry(self, cfg: JobConfig) -> JobTable:
        with self._session() as session:
            row = JobTable(
                id=str(uuid4()),
                name=cfg.name,
                num_of_retries=cfg.num_of_retries,
                file_logging=cfg.file_logging,
                strategy_type=cfg.strategy_type,
            )
            session.add(row)
            session.flush()

            self.dc.create_metadata_for_job(session, row, cfg.metadata_.model_dump())
            self.ch.create_all_from_configs(session, row, cfg.components)

            session.commit()
            session.refresh(row)
            return row

    def update(self, job_id: str, cfg: JobConfig) -> JobTable:
        with self._session() as session:
            row = session.get(JobTable, job_id)
            if row is None:
                raise PersistNotFoundError(f"Job with id {job_id!r} not found")

            row.name = cfg.name
            row.num_of_retries = cfg.num_of_retries
            row.file_logging = cfg.file_logging
            row.strategy_type = cfg.strategy_type
            session.add(row)
            session.flush()

            if row.metadata_ is None:
                self.dc.create_metadata_for_job(
                    session, row, cfg.metadata_.model_dump()
                )
            else:
                self.dc.update_metadata_entry(
                    session, row.metadata_, cfg.metadata_.model_dump()
                )

            self.ch.replace_all_from_configs(session, row, cfg.components)

            session.commit()
            session.refresh(row)
            return row

    def get_by_id(self, job_id: str) -> Optional[JobTable]:
        with self._session() as session:
            return session.get(JobTable, job_id)

    def get_all(self) -> List[JobTable]:
        with self._session() as session:
            return list(session.exec(select(JobTable)).all())

    def get_by_name(self, name: str) -> Optional[JobTable]:
        with self._session() as session:
            stmt = select(JobTable).where(JobTable.name == name)
            return session.exec(stmt).first()

    def _build_runtime_from_record(self, rec: JobTable) -> RuntimeJob:
        """
        Build RuntimeJob and assign components. RuntimeJob wires via `routes`.
        """
        job = RuntimeJob(
            name=rec.name,
            num_of_retries=rec.num_of_retries,
            file_logging=rec.file_logging,
            strategy_type=rec.strategy_type,
            components=[],
            metadata={},
        )
        object.__setattr__(job, "_id", rec.id)

        comps = self.ch.build_runtime_for_all(rec)
        job.components = comps  # validate_assignment triggers RuntimeJob wiring
        if rec.metadata_ is not None:
            job.metadata_ = self.dc.dump_metadata(rec.metadata_)
        return job

    def load_runtime_job(self, job_id: str) -> RuntimeJob:
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
                    # wiring
                    selectinload(JobTable.components)
                    .selectinload(ComponentTable.outgoing_links)
                    .selectinload(ComponentLinkTable.dst_component),
                )
            )
            rec = session.exec(stmt).first()
            if rec is None:
                raise PersistNotFoundError(f"Job with id {job_id!r} not found")
            return self._build_runtime_from_record(rec)

    def list_jobs_brief(self) -> List[Dict[str, Any]]:
        with self._session() as session:
            stmt = select(JobTable).options(
                selectinload(JobTable.metadata_),
                selectinload(JobTable.components).selectinload(ComponentTable.layout),
                selectinload(JobTable.components).selectinload(
                    ComponentTable.metadata_
                ),
            )
            records = session.exec(stmt).all()

            result: List[Dict[str, Any]] = []
            for rec in records:
                job = self._build_runtime_from_record(rec)
                data = job.model_dump(exclude={"components"})
                data["id"] = job.id
                result.append(data)
            return result

    def delete(self, job_id: str) -> None:
        with self._session() as session:
            job = session.get(JobTable, job_id)
            if job is None:
                raise PersistNotFoundError(f"Job with id {job_id!r} not found")
            session.delete(job)
            session.commit()
