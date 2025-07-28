from abc import ABC

from pydantic import BaseModel, Field
from uuid import uuid4


class Receiver(BaseModel, ABC):
    """
    Base class for all receivers in the system
    """

    id: str = Field(default_factory=lambda: str(uuid4()), exclude=True)
