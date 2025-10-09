from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import types

import pytest
from fastapi.testclient import TestClient

import etl_core.main as M
from etl_core.components.component_registry import RegistryMode


class FakeCursor:
    def __init__(self, docs: List[Dict[str, Any]]) -> None:
        self._docs = docs
        self.closed = False

    def limit(self, n: int) -> "FakeCursor":
        return self

    async def to_list(self, length: int) -> List[Dict[str, Any]]:
        return self._docs[:length]

    async def close(self) -> None:
        self.closed = True


class FakeCollection:
    def __init__(self, docs: List[Dict[str, Any]]) -> None:
        self._docs = docs

    def find(self, _query: Dict[str, Any], *, session: Any) -> FakeCursor:
        return FakeCursor(self._docs)


class _Session:
    async def __aenter__(self) -> "_Session":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class FakeAsyncIOMotorClient:
    """Minimal surface needed by main.py."""

    def __init__(self, _uri: str, **_kwargs: Any) -> None:
        self._dbs: Dict[str, Dict[str, FakeCollection]] = {}

    async def start_session(self) -> _Session:
        return _Session()

    def __getitem__(self, db_name: str) -> Dict[str, FakeCollection]:
        return self._dbs.setdefault(db_name, {})

    def close(self) -> None:
        pass


class FakeScheduler:
    _inst: Optional["FakeScheduler"] = None

    def __init__(self) -> None:
        self.started_with: Tuple[()] | Tuple[str] = ()
        self.added_jobs: List[Dict[str, Any]] = []
        self.stopped = False

    @classmethod
    def instance(cls) -> "FakeScheduler":
        if cls._inst is None:
            cls._inst = FakeScheduler()
        return cls._inst

    def start(self, sync_override: Optional[str] = None) -> None:
        if sync_override is None:
            self.started_with = ()
        else:
            self.started_with = (sync_override,)

    def add_internal_job(self, **kwargs: Any) -> None:
        self.added_jobs.append(kwargs)

    async def shutdown_gracefully(self) -> None:
        self.stopped = True


class FakePoolRegistry:
    _inst: Optional["FakePoolRegistry"] = None

    def __init__(self) -> None:
        self.registered: List[Dict[str, Any]] = []

    @classmethod
    def instance(cls) -> "FakePoolRegistry":
        if cls._inst is None:
            cls._inst = FakePoolRegistry()
        return cls._inst

    def register_mongo_client(
        self, *, uri: str, client: Any, client_kwargs: Dict[str, Any]
    ) -> None:
        self.registered.append(
            {"uri": uri, "client": client, "client_kwargs": client_kwargs}
        )


def test_parse_origins_variants() -> None:
    assert M._parse_origins(None) == ["*"]
    assert M._parse_origins("*") == ["*"]
    assert M._parse_origins(" http://a , http://b ") == ["http://a", "http://b"]
    assert M._parse_origins(", ,") == []


def test_resolve_registry_mode_valid_and_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ETL_COMPONENT_MODE", "production")
    assert M._resolve_registry_mode() == RegistryMode.PRODUCTION

    monkeypatch.setenv("ETL_COMPONENT_MODE", "NOT_A_MODE")
    assert M._resolve_registry_mode() == RegistryMode.PRODUCTION


def test_resolve_scheduler_sync_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ETL_SCHEDULES_SYNC_SECONDS", raising=False)
    assert M._resolve_scheduler_sync_seconds() is None

    monkeypatch.setenv("ETL_SCHEDULES_SYNC_SECONDS", "15")
    assert M._resolve_scheduler_sync_seconds() == "15"


def test_resolve_mongo_client_config_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ETL_MONGO_URI", raising=False)
    assert M._resolve_mongo_client_config() == (None, {})


def test_resolve_mongo_client_config_with_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ETL_MONGO_URI", "mongodb://h:27017")
    monkeypatch.setenv("ETL_MONGO_MAX_POOL_SIZE", "42")
    monkeypatch.setenv("ETL_MONGO_WAIT_QUEUE_TIMEOUT_MS", "5000")
    uri, kwargs = M._resolve_mongo_client_config()
    assert uri == "mongodb://h:27017"
    assert kwargs == {"maxPoolSize": 42, "waitQueueTimeoutMS": 5000}


def test_resolve_example_job_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ETL_MONGO_EXAMPLE_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("ETL_MONGO_EXAMPLE_DB", raising=False)
    monkeypatch.delenv("ETL_MONGO_EXAMPLE_COLLECTION", raising=False)
    assert M._resolve_example_job_settings() == (None, None, None)

    monkeypatch.setenv("ETL_MONGO_EXAMPLE_INTERVAL_SECONDS", "7")
    monkeypatch.setenv("ETL_MONGO_EXAMPLE_DB", "db")
    monkeypatch.setenv("ETL_MONGO_EXAMPLE_COLLECTION", "coll")
    assert M._resolve_example_job_settings() == (7, "db", "coll")


@pytest.mark.asyncio
async def test_example_mongo_job_skips_without_client() -> None:
    app = types.SimpleNamespace(state=types.SimpleNamespace(mongo_client=None))
    await M.example_mongo_job(app=app)


@pytest.mark.asyncio
async def test_example_mongo_job_skips_without_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeAsyncIOMotorClient("mongodb://x")
    app = types.SimpleNamespace(state=types.SimpleNamespace(mongo_client=client))
    await M.example_mongo_job(app=app)


@pytest.mark.asyncio
async def test_example_mongo_job_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeAsyncIOMotorClient("mongodb://x")
    client["db"]["coll"] = FakeCollection([{"_id": 1}, {"_id": 2}, {"_id": 3}])

    app = types.SimpleNamespace(
        state=types.SimpleNamespace(
            mongo_client=client,
            example_mongo_db="db",
            example_mongo_collection="coll",
        )
    )
    await M.example_mongo_job(app=app)


def _patch_noops(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(M, "setup_logging", lambda *a, **k: None)
    monkeypatch.setattr(M, "ensure_schema", lambda *a, **k: None)
    monkeypatch.setattr(M, "autodiscover_components", lambda *a, **k: None)
    monkeypatch.setattr(M, "set_registry_mode", lambda *a, **k: None)


def _patch_services(
    monkeypatch: pytest.MonkeyPatch,
) -> Tuple[FakeScheduler, FakePoolRegistry]:
    FakeScheduler._inst = None
    FakePoolRegistry._inst = None

    monkeypatch.setattr(M, "SchedulerService", FakeScheduler)
    monkeypatch.setattr(M, "ConnectionPoolRegistry", FakePoolRegistry)

    return FakeScheduler.instance(), FakePoolRegistry.instance()


def _patch_motor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(M, "AsyncIOMotorClient", FakeAsyncIOMotorClient)


def _reset_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in [
        "ETL_MONGO_URI",
        "ETL_MONGO_MAX_POOL_SIZE",
        "ETL_MONGO_WAIT_QUEUE_TIMEOUT_MS",
        "ETL_SCHEDULES_SYNC_SECONDS",
        "ETL_MONGO_EXAMPLE_INTERVAL_SECONDS",
        "ETL_MONGO_EXAMPLE_DB",
        "ETL_MONGO_EXAMPLE_COLLECTION",
        "ETL_COMPONENT_MODE",
        "CORS_ALLOW_ORIGINS",
    ]:
        monkeypatch.delenv(var, raising=False)


def _ensure_component_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ETL_COMPONENT_MODE", "PRODUCTION")


def test_lifespan_with_mongo_and_example_job(monkeypatch: pytest.MonkeyPatch) -> None:
    _reset_env(monkeypatch)
    _patch_noops(monkeypatch)
    sched, pools = _patch_services(monkeypatch)
    _patch_motor(monkeypatch)

    _ensure_component_mode(monkeypatch)

    monkeypatch.setenv("ETL_MONGO_URI", "mongodb://h:27017")
    monkeypatch.setenv("ETL_MONGO_MAX_POOL_SIZE", "10")
    monkeypatch.setenv("ETL_MONGO_WAIT_QUEUE_TIMEOUT_MS", "1000")
    monkeypatch.setenv("ETL_SCHEDULES_SYNC_SECONDS", "30")
    monkeypatch.setenv("ETL_MONGO_EXAMPLE_INTERVAL_SECONDS", "3")
    monkeypatch.setenv("ETL_MONGO_EXAMPLE_DB", "dbx")
    monkeypatch.setenv("ETL_MONGO_EXAMPLE_COLLECTION", "collx")

    with TestClient(M.app) as client:
        resp = client.get("/__nonexistent__")
        assert resp.status_code == 404

    assert sched.started_with == ("30",)
    assert sched.stopped is True
    assert len(pools.registered) == 1
    assert pools.registered[0]["uri"] == "mongodb://h:27017"
    assert len(sched.added_jobs) == 1
    job = sched.added_jobs[0]
    assert job["job_id"] == M._EXAMPLE_JOB_ID
    assert job["func"] is M.example_mongo_job
    assert "trigger" in job and "kwargs" in job


def test_lifespan_without_mongo(monkeypatch: pytest.MonkeyPatch) -> None:
    _reset_env(monkeypatch)
    _patch_noops(monkeypatch)
    sched, pools = _patch_services(monkeypatch)
    _patch_motor(monkeypatch)

    _ensure_component_mode(monkeypatch)

    with TestClient(M.app):
        pass

    assert sched.started_with in [(), ("",)]
    assert sched.stopped is True
    assert pools.registered == []
