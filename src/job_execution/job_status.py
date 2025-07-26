from enum import Enum


class JobStatus(Enum):
    """
    Enum representing the status of a job
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
