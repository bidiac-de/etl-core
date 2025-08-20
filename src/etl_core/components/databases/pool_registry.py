from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from threading import Lock
from typing import Any, Dict, Mapping, MutableMapping, Optional, Tuple

from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
from pymongo import MongoClient


@dataclass(frozen=True)
class PoolKey:
    """Uniquely identifies a connection pool."""

    kind: str
    dsn: str

    @staticmethod
    def _normalize_pairs(pairs: Mapping[str, Any]) -> Tuple[Tuple[str, Any], ...]:
        items = tuple(sorted(pairs.items(), key=lambda x: x[0]))
        return items

    @classmethod
    def for_sql(
        cls, *, url: str, engine_kwargs: Optional[Mapping[str, Any]] = None
    ) -> "PoolKey":
        engine_kwargs = engine_kwargs or {}
        digest = sha256(
            repr(cls._normalize_pairs(engine_kwargs)).encode("utf-8")
        ).hexdigest()
        return cls(kind="sql", dsn=f"{url}##{digest}")

    @classmethod
    def for_mongo(
        cls, *, uri: str, client_kwargs: Optional[Mapping[str, Any]] = None
    ) -> "PoolKey":
        client_kwargs = client_kwargs or {}
        digest = sha256(
            repr(cls._normalize_pairs(client_kwargs)).encode("utf-8")
        ).hexdigest()
        return cls(kind="mongo", dsn=f"{uri}##{digest}")


class ConnectionPoolRegistry:
    """
    Central registry that creates and reuses SQLAlchemy Engines and MongoClients.
    Keeps simple lease counters for observability and safe shutdown.
    """

    _instance: Optional["ConnectionPoolRegistry"] = None
    _lock: Lock = Lock()

    def __init__(self) -> None:
        self._sql: MutableMapping[PoolKey, Dict[str, Any]] = {}
        self._mongo: MutableMapping[PoolKey, Dict[str, Any]] = {}
        self._guard = Lock()

    @classmethod
    def instance(cls) -> "ConnectionPoolRegistry":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def get_sql_engine(
        self, *, url: str, engine_kwargs: Optional[Mapping[str, Any]] = None
    ) -> Tuple[PoolKey, Engine]:
        engine_kwargs = dict(engine_kwargs or {})
        key = PoolKey.for_sql(url=url, engine_kwargs=engine_kwargs)
        with self._guard:
            slot = self._sql.get(key)
            if slot is None:
                engine = create_engine(url, **engine_kwargs)
                self._sql[key] = {"engine": engine, "leased": 0, "opened": 0}
                slot = self._sql[key]
            return key, slot["engine"]

    def lease_sql(self, key: PoolKey) -> None:
        with self._guard:
            slot = self._sql[key]
            slot["leased"] += 1
            slot["opened"] += 1

    def release_sql(self, key: PoolKey) -> None:
        with self._guard:
            slot = self._sql.get(key)
            if slot:
                slot["leased"] = max(0, slot["leased"] - 1)

    def get_mongo_client(
        self, *, uri: str, client_kwargs: Optional[Mapping[str, Any]] = None
    ) -> Tuple[PoolKey, MongoClient]:
        client_kwargs = dict(client_kwargs or {})
        key = PoolKey.for_mongo(uri=uri, client_kwargs=client_kwargs)
        with self._guard:
            slot = self._mongo.get(key)
            if slot is None:
                client = MongoClient(uri, **client_kwargs)
                self._mongo[key] = {"client": client, "leased": 0, "opened": 0}
                slot = self._mongo[key]
            return key, slot["client"]

    def lease_mongo(self, key: PoolKey) -> None:
        with self._guard:
            slot = self._mongo[key]
            slot["leased"] += 1
            slot["opened"] += 1

    def release_mongo(self, key: PoolKey) -> None:
        with self._guard:
            slot = self._mongo.get(key)
            if slot:
                slot["leased"] = max(0, slot["leased"] - 1)

    def stats(self) -> Dict[str, Dict[str, Dict[str, int]]]:
        with self._guard:
            sql_stats = {
                k.dsn: {"leased": v["leased"], "opened": v["opened"]}
                for k, v in self._sql.items()
            }
            mongo_stats = {
                k.dsn: {"leased": v["leased"], "opened": v["opened"]}
                for k, v in self._mongo.items()
            }
            return {"sql": sql_stats, "mongo": mongo_stats}

    def close_pool(self, key: PoolKey, *, force: bool = False) -> bool:
        with self._guard:
            if key.kind == "sql":
                slot = self._sql.get(key)
                if not slot:
                    return False
                if slot["leased"] > 0 and not force:
                    return False
                slot["engine"].dispose()
                del self._sql[key]
                return True

            if key.kind == "mongo":
                slot = self._mongo.get(key)
                if not slot:
                    return False
                if slot["leased"] > 0 and not force:
                    return False
                slot["client"].close()
                del self._mongo[key]
                return True

            return False
