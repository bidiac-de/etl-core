from __future__ import annotations

from typing import Generator, Callable, Iterable, Tuple, Dict, Any, List, AsyncIterator
import motor.motor_asyncio as motor_asyncio
import pymongo
import pymongo.mongo_client as pymongo_mclient
import mongomock
import pandas as pd
from uuid import uuid4
import asyncio

from tests.utils.async_mongomock import AsyncMongoMockClient
from fastapi.requests import Request
from urllib.parse import urlencode
from pytest import MonkeyPatch

import pytest
import os
import importlib
import secrets
import sys
from fastapi.testclient import TestClient
from sqlmodel import Session, delete
from datetime import datetime, timedelta

from etl_core.main import app
from etl_core.api.dependencies import get_execution_handler, get_job_handler
from etl_core.persistance.db import engine
from etl_core.persistance.table_definitions import (
    JobTable,
    MetaDataTable,
    ComponentTable,
    LayoutTable,
)
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa: E501
    DataOperationsMetrics,
)
from etl_core.components.databases.mongodb.mongodb_connection_handler import (
    MongoConnectionHandler,
)

from etl_core.components.databases.pool_args import build_mongo_client_kwargs
from etl_core.components.databases import pool_args

@pytest.fixture
def data_ops_metrics() -> DataOperationsMetrics:
    """Fresh DataOperationsMetrics for each test."""
    return DataOperationsMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
        lines_dismissed=0,
    )

def _purge_modules(prefixes: Iterable[str]) -> None:
    keys = list(sys.modules.keys())
    to_delete = [
        name
        for name in keys
        if any(name == p or name.startswith(p + ".") for p in prefixes)
    ]
    for name in to_delete:
        sys.modules.pop(name, None)


@pytest.fixture
def fresh_client(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> Callable[[str], TestClient]:
    """
    Build a brand‑new FastAPI TestClient for the given mode and ENSURE:
      - all app, component, and router modules are re-imported cleanly
      - the app lifespan runs (so set_registry_mode(...) takes effect)
    """

    def _make(mode: str) -> TestClient:
        monkeypatch.setenv("ETL_COMPONENT_MODE", mode)
        _purge_modules(
            (
                "etl_core.main",
                "etl_core.components",  # component registry and implementations
                "etl_core.api.routers",  # routers (schemas/jobs/execution/setup)
            )
        )
        importlib.invalidate_caches()
        import etl_core.main as main

        client = TestClient(main.app)
        client.__enter__()  # run lifespan
        request.addfinalizer(lambda: client.__exit__(None, None, None))
        return client

    return _make


@pytest.fixture()
def client() -> Generator[TestClient, None, None]:
    """
    Use TestClient as a context manager so FastAPI lifespan runs,
    which creates the shared handlers on app.state.
    """
    with TestClient(app) as c:
        yield c


@pytest.fixture
def metrics() -> ComponentMetrics:
    return ComponentMetrics(
        started_at=datetime.now(),
        processing_time=timedelta(0),
        error_count=0,
        lines_received=0,
        lines_forwarded=0,
    )


@pytest.fixture(scope="session", autouse=True)
def enable_stub_components() -> None:
    """
    Automatically run before any tests.
    Sets the registry mode to 'test' so stub components are visible.
    """
    os.environ["ETL_COMPONENT_MODE"] = "test"
    if "etl_core.main" in sys.modules:
        import etl_core.main

        importlib.reload(etl_core.main)


@pytest.fixture(autouse=True)
def clear_db() -> Generator[None, None, None]:
    """
    Ensure a clean DB before/after each test.
    """
    with Session(engine) as session:
        session.exec(delete(JobTable))
        session.exec(delete(MetaDataTable))
        session.exec(delete(ComponentTable))
        session.exec(delete(LayoutTable))
        session.commit()
    yield
    with Session(engine) as session:
        session.exec(delete(JobTable))
        session.exec(delete(MetaDataTable))
        session.exec(delete(ComponentTable))
        session.exec(delete(LayoutTable))
        session.commit()


@pytest.fixture()
def shared_job_handler(client: TestClient):
    req = Request({"type": "http", "app": client.app})
    return get_job_handler(req)


@pytest.fixture()
def reset_overrides() -> Generator[None, None, None]:
    """
    Clear FastAPI dependency overrides after each test so they don't leak.
    """
    try:
        yield
    finally:
        app.dependency_overrides.clear()


@pytest.fixture()
def override_job_handler() -> Generator[None, None, None]:
    """
    Example: override JobHandler DI for a test.
    Usage:
        class FakeJobHandler: ...
        app.dependency_overrides[get_job_handler] = lambda: FakeJobHandler()
    """
    try:
        yield
    finally:
        if get_job_handler in app.dependency_overrides:
            del app.dependency_overrides[get_job_handler]


@pytest.fixture()
def override_exec_handler() -> Generator[None, None, None]:
    """
    Example: override JobExecutionHandler DI for a test.
    Usage:
        class FakeExec: ...
        app.dependency_overrides[get_execution_handler] = lambda: FakeExec()
    """
    try:
        yield
    finally:
        if get_execution_handler in app.dependency_overrides:
            del app.dependency_overrides[get_execution_handler]


@pytest.fixture(scope="session", autouse=True)
def _set_test_env() -> Tuple[str, str]:
    """
    Session-wide: ensure test creds exist in the environment exactly once,
    without using the function-scoped 'monkeypatch'.
    """
    user = os.environ.get("APP_TEST_USER") or "test_user"
    password = os.environ.get("APP_TEST_PASSWORD") or secrets.token_urlsafe(24)
    os.environ["APP_TEST_USER"] = user
    os.environ["APP_TEST_PASSWORD"] = password
    return user, password


@pytest.fixture()
def test_creds(_set_test_env: Tuple[str, str]) -> Tuple[str, str]:
    """
    Function-scoped handle for tests that need the values.
    Reads from env guaranteed by '_set_test_env'.
    """
    return os.environ["APP_TEST_USER"], os.environ["APP_TEST_PASSWORD"]


@pytest.fixture()
def schema_row_min() -> Dict[str, object]:
    """
    Minimal row schema used by most tests.
    Named to allow adding more schema variants later (e.g., schema_row_with_meta).
    """
    return {"fields": [{"name": "id", "data_type": "integer", "nullable": False}]}


@pytest.fixture()
def schema_row_two_fields() -> Dict[str, object]:
    """
    Example extension for future tests: two-field integer schema.
    Not used by current tests, but here to demonstrate the pattern.
    """
    return {
        "fields": [
            {"name": "id", "data_type": "integer", "nullable": False},
            {"name": "value", "data_type": "integer", "nullable": True},
        ]
    }


class _DummyCreds:
    def __init__(self) -> None:
        self._params: Dict[str, Any] = {
            "user": "u",
            "host": "localhost",
            "port": 27017,
            "database": "testdb",
        }
        self._password = "p"

    def get_parameter(self, key: str) -> Any:
        return self._params.get(key)

    @property
    def decrypted_password(self) -> str:
        return self._password


class _DummyContext:
    def __init__(self, creds: _DummyCreds) -> None:
        self._creds = creds

    def get_credentials(self, _: int) -> _DummyCreds:
        return self._creds


@pytest.fixture(autouse=True)
def patch_mongo_clients(monkeypatch: pytest.MonkeyPatch) -> None:
    # Motor async client -> async mongomock wrapper
    monkeypatch.setattr(
        motor_asyncio,
        "AsyncIOMotorClient",
        AsyncMongoMockClient,
        raising=True,
    )

    import etl_core.components.databases.pool_registry as pool_registry_mod

    monkeypatch.setattr(
        pool_registry_mod,
        "AsyncIOMotorClient",
        AsyncMongoMockClient,
        raising=True,
    )

    monkeypatch.setattr(pymongo, "MongoClient", mongomock.MongoClient, raising=True)
    monkeypatch.setattr(
        pymongo_mclient, "MongoClient", mongomock.MongoClient, raising=True
    )


@pytest.fixture
def mongo_context() -> _DummyContext:
    return _DummyContext(_DummyCreds())


@pytest.fixture
def sample_docs() -> list[dict]:
    return [
        {"_id": 1, "name": "John", "age": 28, "city": "Berlin", "active": True},
        {"_id": 2, "name": "Jane", "age": 31, "city": "Hamburg", "active": True},
        {"_id": 3, "name": "Bob", "age": 22, "city": "Berlin", "active": False},
        {"_id": 4, "name": "Alice", "age": 27, "city": "Munich", "active": True},
    ]


@pytest.fixture
def sample_pdf(sample_docs: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(sample_docs)


async def seed_docs(
    handler: MongoConnectionHandler,
    database: str,
    collection: str,
    docs: List[Dict[str, Any]],
) -> None:
    """Insert many docs using the real handler directly (no components)."""
    if not docs:
        return
    with handler.lease_collection(database=database, collection=collection) as (
        _,
        coll,
    ):
        await coll.insert_many(list(docs), ordered=True)


async def get_all_docs(
    handler: MongoConnectionHandler,
    database: str,
    collection: str,
    *,
    flt: Dict[str, Any] | None = None,
    projection: Dict[str, int] | None = None,
) -> List[Dict[str, Any]]:
    """Read all docs via the async cursor using the real handler directly."""
    out: List[Dict[str, Any]] = []
    with handler.lease_collection(database=database, collection=collection) as (
        _,
        coll,
    ):
        cursor = coll.find(
            filter=flt or {}, projection=projection, no_cursor_timeout=False
        )
        async for doc in cursor:
            out.append(doc)
    return out


@pytest.fixture
async def mongo_handler(
    mongo_context,
) -> AsyncIterator[Tuple[MongoConnectionHandler, str]]:
    """
    Provide a fresh Mongo client + a UNIQUE database name per test to avoid
    cross-test data bleed (mongomock keeps data alive across pooled clients).
    Also align the component credentials database to that unique DB so
    components and direct handler calls see the same data.
    """
    creds = mongo_context.get_credentials(101)
    uri = MongoConnectionHandler.build_uri(
        user=creds.get_parameter("user"),
        password=creds.decrypted_password,
        host=creds.get_parameter("host"),
        port=creds.get_parameter("port"),
        auth_db=None,
        params=None,
    )
    client_kwargs = build_mongo_client_kwargs(creds)

    handler = MongoConnectionHandler()
    handler.connect(uri=uri, client_kwargs=client_kwargs)

    dbname = f"testdb_{uuid4().hex}"

    #  make the components use the same DB as the seeding helper
    creds._params["database"] = dbname  # noqa: SLF001

    try:
        yield handler, dbname
    finally:
        try:
            client = handler._client  # noqa: SLF001
            if client is not None:
                client.drop_database(dbname)
        except Exception:
            pass
        handler.close_pool(force=True)
        await asyncio.sleep(0)


@pytest.fixture(scope="session", autouse=True)
def _force_mongomock_no_auth():
    """
    Session-wide patch: build *no-auth* Mongo URIs and strip username/password
    from client kwargs to keep mongomock happy (no SCRAM, no 'mongodb://:@...').
    """
    mp = MonkeyPatch()

    def _build_uri_no_auth(user, password, host, port, auth_db, params):
        base = f"mongodb://{host}:{port}"
        if auth_db:
            base = f"{base}/{auth_db}"
        if params:
            if isinstance(params, dict) and params:
                qs = urlencode(params, doseq=True)
                base = f"{base}?{qs}"
            elif isinstance(params, str) and params:
                sep = "&" if "?" in base else "?"
                base = f"{base}{sep}{params}"
        return base

    mp.setattr(
        MongoConnectionHandler,
        "build_uri",
        staticmethod(_build_uri_no_auth),
        raising=True,
    )

    # Strip auth keys defensively from client kwargs
    _orig_build_kwargs = pool_args.build_mongo_client_kwargs

    def _kwargs_no_auth(creds):
        kw = _orig_build_kwargs(creds)
        kw.pop("username", None)
        kw.pop("password", None)
        return kw

    mp.setattr(pool_args, "build_mongo_client_kwargs", _kwargs_no_auth, raising=True)

    # apply for entire session
    yield
    mp.undo()
