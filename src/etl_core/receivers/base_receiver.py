from abc import ABC

from pydantic import BaseModel, PrivateAttr
from uuid import uuid4


class Receiver(BaseModel, ABC):
    """
    Base class for all receivers in the system
    """

    _id: str = PrivateAttr(default_factory=lambda: str(uuid4()))
