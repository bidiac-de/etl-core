from __future__ import annotations

import threading
from typing import Dict

from src.context.secrets.secret_provider import SecretProvider


class InMemorySecretProvider(SecretProvider):
    """
    Process-local, thread-safe secret store; use only in tests/CI.
    """

    def __init__(self) -> None:
        self._store: Dict[str, str] = {}
        self._lock = threading.RLock()

    def get(self, key: str) -> str:
        with self._lock:
            try:
                return self._store[key]
            except KeyError as exc:
                raise KeyError(f"Secret not found for key='{key}'.") from exc

    def set(self, key: str, secret: str) -> None:
        with self._lock:
            self._store[key] = secret

    def exists(self, key: str) -> bool:
        with self._lock:
            return key in self._store

    def delete(self, key: str) -> None:
        # Idempotent delete: no error if missing
        with self._lock:
            self._store.pop(key, None)
