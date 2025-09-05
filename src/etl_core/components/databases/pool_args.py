from __future__ import annotations

from typing import Any, Dict, Optional
from etl_core.context.context_provider import IContextProvider


def _get_int(provider: IContextProvider, key: str) -> Optional[int]:
    try:
        val = provider.get_parameter(key)
    except Exception:
        return None
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def build_sql_engine_kwargs(provider: IContextProvider) -> Dict[str, Any]:
    """
    Map portable pool_* params to SQLAlchemy engine kwargs.
    Only set keys if provided; otherwise let SQLAlchemy defaults apply.
    """
    kwargs: Dict[str, Any] = {}

    max_size = _get_int(provider, "pool_max_size")
    timeout_s = _get_int(provider, "pool_timeout_s")

    if max_size is not None:
        kwargs["pool_size"] = max_size
    if timeout_s is not None:
        kwargs["pool_timeout"] = timeout_s

    return kwargs


def build_mongo_client_kwargs(provider: IContextProvider) -> Dict[str, Any]:
    """
    Map portable pool_* params to PyMongo MongoClient kwargs.
    Only set keys if provided; otherwise let PyMongo defaults apply.
    """
    kwargs: Dict[str, Any] = {}

    max_size = _get_int(provider, "pool_max_size")
    timeout_s = _get_int(provider, "pool_timeout_s")

    if max_size is not None:
        kwargs["maxPoolSize"] = max_size
    if timeout_s is not None:
        kwargs["waitQueueTimeoutMS"] = timeout_s * 1000

    return kwargs
