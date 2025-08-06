from typing import List, Optional, Any
import datetime

from sqlmodel import Session, select
from src.persistance.db import engine
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
    def _deep_serialize(obj: Any) -> Any:
        """
        Recursively convert datetime, date, time to ISO strings,
        and walk through lists/dicts.
        """
        if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return obj.isoformat()
        if isinstance(obj, dict):
            return {k: JobHandler._deep_serialize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [JobHandler._deep_serialize(v) for v in obj]
        return obj

    @classmethod
    def _serialize_components(cls, components: List[Component]) -> List[dict]:
        """
        Serialize each Component via model_dump and deep-serialize it.
        """
        out: List[dict] = []
        for comp in components:
            data = comp.model_dump()
            out.append(cls._deep_serialize(data))
        return out


    @classmethod
    def _serialize_metadata(cls, meta: Any) -> dict:
        """
        Serialize metadata (dataclass → asdict, else Pydantic → model_dump)
        then deep-serialize to handle datetimes.
        """
        data = meta.model_dump()
        return cls._deep_serialize(data)

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

    def get_all(self) -> list[JobTable]:
        """
        Return every JobTable row as a concrete list.
        """
        with Session(self.engine) as session:
            records = session.exec(select(JobTable)).all()
            #cast to list for strict typing
            return list(records)

    @staticmethod
    def record_to_job(record: JobTable) -> Job:
        """
        Build a Job domain object from a JobTable record
        """
        payload = {
            "name": record.name,
            "num_of_retries": record.num_of_retries,
            "file_logging": record.file_logging,
            "strategy_type": record.strategy_type,
            "components": record.components,
            "metadata": record.metadata_,
        }
        # Instantiate Pydantic Job
        job = Job(**payload)
        # Override private _id to match the record's id
        object.__setattr__(job, "_id", record.id)
        return job