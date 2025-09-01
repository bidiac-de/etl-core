from abc import ABC, abstractmethod


class IContextProvider(ABC):
    @abstractmethod
    def get_parameter(self, key: str) -> str:
        """Retrieve a parameter value by key."""
        pass

    # Optional lifecycle hook: providers that write secrets can override this.
    def delete_from_store(self) -> None:
        """
        Optional cleanup hook to remove provider-owned secrets from the backing
        secret store. Default is a no-op so non-secret providers don't need to
        implement anything.
        """
        return None
