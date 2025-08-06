import uuid
from typing import Optional
from sqlmodel import Session, select

from src.job_execution.job import Job


class JobHandler:
    """
    CRUD operations for Job (and its Components) using SQLModel.
    """

    def __init__(self, session: Session):
        self.session = session

    def create(self, job: Job) -> Job:
        """
        Persist a new Job (with its components) and return the saved instance.
        """
        self.session.add(job)
        self.session.commit()
        self.session.refresh(job)
        return job

    def get(self, job_id: uuid.UUID) -> Optional[Job]:
        """
        Retrieve a Job by its UUID (or None if not found).
        """
        statement = select(Job).where(Job.id == job_id)
        return self.session.exec(statement).one_or_none()

    def update(self, job_id: uuid.UUID, job_data: Job) -> Optional[Job]:
        """
        Update an existing Job. Pass in a Job model with the same .id,
        or return None if the original doesn't exist.
        """
        existing = self.get(job_id)
        if not existing:
            return None

        # merge fields (you can also use .update from dicts)
        for field, value in job_data.model_dump(exclude_unset=True).items():
            setattr(existing, field, value)

        self.session.add(existing)
        self.session.commit()
        self.session.refresh(existing)
        return existing

    def delete(self, job_id: uuid.UUID) -> bool:
        """
        Delete a Job by UUID. Returns True if deleted, False if not found.
        """
        existing = self.get(job_id)
        if not existing:
            return False
        self.session.delete(existing)
        self.session.commit()
        return True