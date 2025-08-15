from abc import ABC, abstractmethod


class IContextProvider(ABC):
    @abstractmethod
    def get_parameter(self, key: str) -> str:
        """Retrieve a parameter value by key."""
        pass
