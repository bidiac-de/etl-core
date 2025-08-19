from enum import Enum


class RuntimeState(Enum):
    """
    Runtime state of a component during execution
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
