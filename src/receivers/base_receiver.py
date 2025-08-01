from abc import ABC


class Receiver(ABC):
    """Base class for all receivers."""
    def __init__(self, id: int = 0):
        self.id = id
