from typing import List, Optional

from sqlmodel import Session
from db import engine
from src.persistance.table_definitions import JobTable
from src.job_execution.job import Job
from src.components.base_component import Component


class JobHandler:
    """
    CRUD for JobTable, serializing via Pydantic v2’s model_dump().
    """

    def __init__(self, session_engine=engine):
        self.engine = session_engine

    @staticmethod
    def _serialize_components(components: List[Component]) -> List[dict]:
        # every Component is a Pydantic model → use model_dump()
        return [comp.model_dump() for comp in components]

    @staticmethod
    def _serialize_metadata(meta) -> dict:
        return meta.model_dump()

    def create(self, job: Job) -> JobTable:
        """
        INSERT a new row for this Job.
        """
        # build a JobTable instance
        job_record = JobTable(
            id=job.id,
            name=job.name,
            num_of_retries=job.num_of_retries,
            file_logging=job.file_logging,
            strategy_type=job.strategy_type,
            components=self._serialize_components(job.components),
            metadata_=self._serialize_metadata(job.metadata),
        )
        with Session(self.engine) as session:
            session.add(job_record)
            session.commit()
            session.refresh(job_record)
            return job_record  # ← instance, never the class

    def update(self, job: Job):
        """
        UPDATE the row matching job.id with this Job’s fields.
        """
        with Session(self.engine) as session:
            record = session.get(JobTable, job.id)
            if record is None:
                raise ValueError(f"Job with id {job.id!r} not found")
            # assign every field
            record.name = job.name
            record.num_of_retries = job.num_of_retries
            record.file_logging = job.file_logging
            record.strategy_type = job.strategy_type
            record.components = self._serialize_components(job.components)
            record.metadata_ = self._serialize_metadata(job.metadata)
            session.add(record)
            session.commit()
            session.refresh(record)
            return record  # ← still an instance

    def delete(self, job_id: str) -> None:
        """
        DELETE the row with the given id.
        """
        with Session(self.engine) as session:
            record = session.get(JobTable, job_id)
            if record is None:
                raise ValueError(f"Job with id {job_id!r} not found")
            session.delete(record)
            session.commit()

    def get_by_id(self, job_id: str) -> Optional[JobTable]:
        """
        SELECT * FROM job_table WHERE id = :job_id.
        """
        with Session(self.engine) as session:
            return session.get(JobTable, job_id)
