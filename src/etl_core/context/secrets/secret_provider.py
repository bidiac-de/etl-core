from __future__ import annotations
from abc import ABC, abstractmethod


class SecretProvider(ABC):
    """
    Abstract interface for a secure secret store.
    """

    @abstractmethod
    def get(self, key: str) -> str:
        """
        Return the plaintext secret for `key`.
        """
        raise NotImplementedError

    @abstractmethod
    def set(self, key: str, secret: str) -> None:
        """
        Persist `secret` under `key` in the secure store.
        """
        raise NotImplementedError

    @abstractmethod
    def exists(self, key: str) -> bool:
        """
        Check if a secret exists in the store.
        """
        raise NotImplementedError

    @abstractmethod
    def delete(self, key: str) -> None:
        """
        Delete the secret for `key`.
        """
        raise NotImplementedError
