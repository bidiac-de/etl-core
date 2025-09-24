from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass
from hashlib import sha256
from threading import Lock
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Tuple

from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


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
    """Create and reuse SQLAlchemy Engines and AsyncIOMotorClient instances."""

    _instance: Optional["ConnectionPoolRegistry"] = None
    _lock: Lock = Lock()

    def __init__(self) -> None:
        self._sql: MutableMapping[PoolKey, Dict[str, Any]] = {}
        self._mongo: MutableMapping[PoolKey, Dict[str, Any]] = {}
        self._guard = Lock()
        self._log = logging.getLogger("etl_core.components.databases.pool_registry")
        self._idle_timeout_seconds = self._load_idle_timeout()

    @staticmethod
    def _load_idle_timeout() -> int:
        raw = os.getenv("ETL_POOL_IDLE_TIMEOUT_SECONDS")
        default = 300
        if raw is None:
            return default
        try:
            value = int(raw)
        except ValueError:
            return default
        return value if value >= 0 else default

    @classmethod
    def instance(cls) -> "ConnectionPoolRegistry":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def _handle_idle_timeout(self, key: PoolKey) -> None:
        try:
            closed = self.close_pool(key)
            if closed:
                self._log.debug("Closed idle %s pool %s", key.kind, key.dsn)
        except Exception:  # pragma: no cover - cleanup should not crash caller
            self._log.exception("Failed to close idle pool %s", key.dsn)

    def _schedule_idle_close(self, key: PoolKey, slot: Dict[str, Any]) -> None:
        if self._idle_timeout_seconds == 0:
            delay = 0.01
        else:
            delay = float(self._idle_timeout_seconds)
        timer = threading.Timer(delay, self._handle_idle_timeout, args=(key,))
        timer.daemon = True
        slot["close_timer"] = timer
        timer.start()

    @staticmethod
    def _cancel_timer(slot: Dict[str, Any]) -> None:
        timer = slot.get("close_timer")
        if timer:
            slot["close_timer"] = None
            timer.cancel()

    def get_sql_engine(
        self, *, url: str, engine_kwargs: Optional[Mapping[str, Any]] = None
    ) -> Tuple[PoolKey, Engine]:
        engine_kwargs = dict(engine_kwargs or {})
        key = PoolKey.for_sql(url=url, engine_kwargs=engine_kwargs)
        with self._guard:
            slot = self._sql.get(key)
            if slot is None:
                engine = create_engine(url, **engine_kwargs)
                slot = {
                    "engine": engine,
                    "leased": 0,
                    "opened": 0,
                    "close_timer": None,
                }
                self._sql[key] = slot
            else:
                self._cancel_timer(slot)
            return key, slot["engine"]

    def lease_sql(self, key: PoolKey) -> None:
        timer_to_cancel: Optional[threading.Timer] = None
        with self._guard:
            slot = self._sql.get(key)
            if slot is None:
                return
            slot["leased"] += 1
            slot["opened"] += 1
            timer_to_cancel = slot.get("close_timer")
            slot["close_timer"] = None
        if timer_to_cancel:
            timer_to_cancel.cancel()

    def release_sql(self, key: PoolKey) -> None:
        timer_to_cancel: Optional[threading.Timer] = None
        with self._guard:
            slot = self._sql.get(key)
            if not slot:
                return
            slot["leased"] = max(0, slot["leased"] - 1)
            timer_to_cancel = slot.get("close_timer")
            slot["close_timer"] = None
            if slot["leased"] == 0 and self._idle_timeout_seconds >= 0:
                self._schedule_idle_close(key, slot)
        if timer_to_cancel:
            timer_to_cancel.cancel()

    def get_mongo_client(
        self, *, uri: str, client_kwargs: Optional[Mapping[str, Any]] = None
    ) -> Tuple[PoolKey, Any]:
        client_kwargs = dict(client_kwargs or {})
        key = PoolKey.for_mongo(uri=uri, client_kwargs=client_kwargs)
        with self._guard:
            slot = self._mongo.get(key)
            if slot is None:
                client = AsyncIOMotorClient(uri, **client_kwargs)
                slot = {
                    "client": client,
                    "leased": 0,
                    "opened": 0,
                    "close_timer": None,
                }
                self._mongo[key] = slot
            else:
                self._cancel_timer(slot)
            return key, slot["client"]

    def register_mongo_client(
        self,
        *,
        uri: str,
        client: AsyncIOMotorClient,
        client_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> PoolKey:
        client_kwargs = dict(client_kwargs or {})
        key = PoolKey.for_mongo(uri=uri, client_kwargs=client_kwargs)
        with self._guard:
            slot = self._mongo.get(key)
            if slot is not None and slot["client"] is not client:
                try:
                    slot["client"].close()
                except Exception:
                    pass
            self._mongo[key] = {
                "client": client,
                "leased": 0,
                "opened": 0,
                "close_timer": None,
            }
        return key

    def lease_mongo(self, key: PoolKey) -> None:
        timer_to_cancel: Optional[threading.Timer] = None
        with self._guard:
            slot = self._mongo.get(key)
            if slot is None:
                return
            slot["leased"] += 1
            slot["opened"] += 1
            timer_to_cancel = slot.get("close_timer")
            slot["close_timer"] = None
        if timer_to_cancel:
            timer_to_cancel.cancel()

    def release_mongo(self, key: PoolKey) -> None:
        timer_to_cancel: Optional[threading.Timer] = None
        with self._guard:
            slot = self._mongo.get(key)
            if not slot:
                return
            slot["leased"] = max(0, slot["leased"] - 1)
            timer_to_cancel = slot.get("close_timer")
            slot["close_timer"] = None
            if slot["leased"] == 0 and self._idle_timeout_seconds >= 0:
                self._schedule_idle_close(key, slot)
        if timer_to_cancel:
            timer_to_cancel.cancel()

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
        timer_to_cancel: Optional[threading.Timer] = None
        with self._guard:
            if key.kind == "sql":
                slot = self._sql.get(key)
                if slot is None:
                    return False
                if slot["leased"] > 0 and not force:
                    return False
                timer_to_cancel = slot.get("close_timer")
                slot["close_timer"] = None
                slot["engine"].dispose()
                del self._sql[key]
                closed = True
            elif key.kind == "mongo":
                slot = self._mongo.get(key)
                if slot is None:
                    return False
                if slot["leased"] > 0 and not force:
                    return False
                timer_to_cancel = slot.get("close_timer")
                slot["close_timer"] = None
                slot["client"].close()
                del self._mongo[key]
                closed = True
            else:
                return False
        if timer_to_cancel:
            timer_to_cancel.cancel()
        return closed

    def close_idle_pools(self) -> Dict[str, int]:
        sql_keys: List[PoolKey]
        mongo_keys: List[PoolKey]
        with self._guard:
            sql_keys = [key for key, slot in self._sql.items() if slot["leased"] == 0]
            mongo_keys = [
                key for key, slot in self._mongo.items() if slot["leased"] == 0
            ]
        closed_counts = {"sql": 0, "mongo": 0}
        for key in sql_keys:
            if self.close_pool(key):
                closed_counts["sql"] += 1
        for key in mongo_keys:
            if self.close_pool(key):
                closed_counts["mongo"] += 1
        return closed_counts
