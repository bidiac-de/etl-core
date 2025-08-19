from __future__ import annotations

import threading
from typing import Dict, List

from src.etl_core.context.context_provider import IContextProvider


class ContextRegistry:
    """
    Thread-safe in-process registry mapping credentials_id -> IContextProvider.
    """

    _lock = threading.RLock()
    _providers: Dict[str, IContextProvider] = {}

    @classmethod
    def register(cls, credentials_id: str, provider: IContextProvider) -> None:
        with cls._lock:
            cls._providers[credentials_id] = provider

    @classmethod
    def resolve(cls, credentials_id: str) -> IContextProvider:
        with cls._lock:
            try:
                return cls._providers[credentials_id]
            except KeyError as exc:
                raise KeyError(
                    f"Context provider not found for credentials_id='{credentials_id}'."
                ) from exc

    @classmethod
    def unregister(cls, credentials_id: str) -> None:
        with cls._lock:
            cls._providers.pop(credentials_id, None)

    @classmethod
    def list_ids(cls) -> List[str]:
        """
        Snapshot of all registered provider IDs.
        """
        with cls._lock:
            return list(cls._providers.keys())

    @classmethod
    def clear(cls) -> None:
        with cls._lock:
            cls._providers.clear()
