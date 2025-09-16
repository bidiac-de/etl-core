from __future__ import annotations

from datetime import datetime, timedelta
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Tuple,
)
from uuid import uuid4
from urllib.parse import urlencode
import asyncio
import importlib
import os
import secrets
import sys
import shutil
import tempfile
from pathlib import Path

import pandas as pd
import pytest
from fastapi.requests import Request
from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from sqlmodel import Session, delete

from etl_core.main import app
from etl_core.api.dependencies import get_execution_handler, get_job_handler
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.metrics.component_metrics.data_operations_metrics.data_operations_metrics import (  # noqa: E501
    DataOperationsMetrics,
)
from etl_core.persistence.db import engine
from etl_core.persistence.table_definitions import (
    ComponentTable,
    JobTable,
    LayoutTable,
    MetaDataTable,
)

from etl_core.components.databases.mongodb.mongodb_connection_handler import (
    MongoConnectionHandler,
)
from etl_core.components.databases import pool_args
from etl_core.components.databases.pool_args import build_mongo_client_kwargs


from etl_core.context.environment import Environment
from etl_core.context.credentials import Credentials
from etl_core.singletons import (
    credentials_handler as _crh_singleton,
    context_handler as _ch_singleton,
)

_TEST_LOG_DIR = Path(tempfile.mkdtemp(prefix="etl-core-logs-")).resolve()
os.environ.setdefault("LOG_DIR", str(_TEST_LOG_DIR))


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
    """
    Remove modules from sys.modules whose names match or start with
     any of the given prefixes.

    This allows re-importing those modules to pick up environment overrides
    cleanly.

    Args:
        prefixes: An iterable of string prefixes. Any module whose name is equal to a
        prefix or starts with a prefix followed by a dot will be removed
        from sys.modules.
    """
    keys = list(sys.modules.keys())
    to_delete = [
        name
        for name in keys
        if any(name == p or name.startswith(p + ".") for p in prefixes)
    ]
    for name in to_delete:
        sys.modules.pop(name, None)


@pytest.fixture(scope="session", autouse=True)
def _cleanup_test_logs() -> Generator[None, None, None]:
    """Ensure temporary log directory does not leak between runs."""

    try:
        yield
    finally:
        shutil.rmtree(_TEST_LOG_DIR, ignore_errors=True)


@pytest.fixture
def fresh_client(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> Callable[[str], TestClient]:
    """
    Build a TestClient after setting ETL_COMPONENT_MODE.
    Use like: client = fresh_client("test")  # or "production"
    """

    def _make(mode: str) -> TestClient:
        monkeypatch.setenv("ETL_COMPONENT_MODE", mode)
        _purge_modules(
            (
                "etl_core.main",
                "etl_core.components",
                "etl_core.api.routers",
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
    Ensure component mode is 'test' for the suite unless explicitly overridden.
    """
    os.environ["ETL_COMPONENT_MODE"] = "test"
    if "etl_core.main" in sys.modules:
        import etl_core.main

        importlib.reload(etl_core.main)


@pytest.fixture(autouse=True)
def clear_db() -> Generator[None, None, None]:
    """
    Truncate core persistence tables before and after each test.
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
    """
    Convenience access to the JobHandler resolved via FastAPI dependencies.
    """
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
    Provide a username/password pair for tests via env.
    If not present, generate a password.
    """
    user = os.environ.get("APP_TEST_USER") or "test_user"
    password = os.environ.get("APP_TEST_PASSWORD") or secrets.token_urlsafe(24)
    os.environ["APP_TEST_USER"] = user
    os.environ["APP_TEST_PASSWORD"] = password
    return user, password


@pytest.fixture()
def test_creds(_set_test_env: Tuple[str, str]) -> Tuple[str, str]:
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


@pytest.fixture
def sample_docs() -> List[Dict[str, Any]]:
    return [
        {"_id": 1, "name": "John", "age": 28, "city": "Berlin", "active": True},
        {"_id": 2, "name": "Jane", "age": 31, "city": "Hamburg", "active": True},
        {"_id": 3, "name": "Bob", "age": 22, "city": "Berlin", "active": False},
        {"_id": 4, "name": "Alice", "age": 27, "city": "Munich", "active": True},
    ]


@pytest.fixture
def sample_pdf(sample_docs: List[Dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(sample_docs)


@pytest.fixture()
def persisted_mongo_credentials(test_creds: Tuple[str, str]) -> Tuple[Credentials, str]:
    """
    Persist real Credentials for Mongo tests (host/port/db are local/mocked).
    provider_id == credentials_id to satisfy FK to mapping table.
    """
    user, password = test_creds
    creds = Credentials(
        credentials_id=str(uuid4()),
        name="mongo_test_creds",
        user=user,
        host="localhost",
        port=27017,
        database="testdb_placeholder",
        password=password,
        pool_max_size=10,
        pool_timeout_s=30,
    )
    credentials_id = _crh_singleton().upsert(creds)
    return creds, credentials_id


@pytest.fixture()
def persisted_mongo_context_id(
    persisted_mongo_credentials: Tuple[Credentials, str],
) -> str:
    """
    Create a Credentials-Mapping Context that maps TEST to persisted Mongo creds.
    Return the context provider_id, which components use as context_id.
    """
    _, persisted_mongo_creds_id = persisted_mongo_credentials
    context_id = str(uuid4())
    _ch_singleton().upsert_credentials_mapping_context(
        context_id=context_id,
        name="mongo_test_context",
        environment=Environment.TEST.value,
        mapping_env_to_credentials_id={
            Environment.TEST.value: persisted_mongo_creds_id
        },
    )
    return context_id


async def seed_docs(
    handler: MongoConnectionHandler,
    database: str,
    collection: str,
    docs: List[Dict[str, Any]],
) -> None:
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
    persisted_mongo_credentials: Tuple[Credentials, str],
) -> AsyncIterator[Tuple[MongoConnectionHandler, str]]:
    """
    Build a MongoConnectionHandler using the persisted credentials.
    Assign a fresh DB name per test and reflect it into the persisted creds
    so components resolve the same database.
    """
    #  Connect using current persisted creds (host/port/user/pass)
    persisted_mongo_credentials, creds_id = persisted_mongo_credentials
    dbname = f"testdb_{uuid4().hex}"
    persisted_mongo_credentials.database = dbname
    _crh_singleton().upsert(
        credentials_id=creds_id,
        creds=persisted_mongo_credentials,
    )

    uri = MongoConnectionHandler.build_uri(
        host=persisted_mongo_credentials.get_parameter("host"),
        port=persisted_mongo_credentials.get_parameter("port"),
        user=persisted_mongo_credentials.get_parameter("user"),
        password=persisted_mongo_credentials.decrypted_password,
        auth_db=dbname,
        params=None,
    )
    client_kwargs = build_mongo_client_kwargs(persisted_mongo_credentials)

    handler = MongoConnectionHandler()
    handler.connect(uri=uri, client_kwargs=client_kwargs)

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
def _force_mongomock_no_auth() -> Generator[None, None, None]:
    """
    Many CI/test setups rely on mongomock-compatible URIs without auth.
    Patch the URI builder and pool args to avoid injecting username/password.
    """
    mp = MonkeyPatch()

    from tests.async_mongomock import AsyncMongoMockClient

    def _build_uri_no_auth(
        *,
        host: str,
        port: int,
        user: str | None = None,
        password: str | None = None,
        auth_db: str | None = None,
        params: Dict[str, Any] | None = None,
    ) -> str:
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

    _orig_build_kwargs = pool_args.build_mongo_client_kwargs

    def _kwargs_no_auth(creds_obj: Credentials) -> Dict[str, Any]:
        kw = _orig_build_kwargs(creds_obj)
        kw.pop("username", None)
        kw.pop("password", None)
        return kw

    mp.setattr(pool_args, "build_mongo_client_kwargs", _kwargs_no_auth, raising=True)

    # Ensure async Mongo connections use an in-memory mongomock client
    from etl_core.components.databases import pool_registry

    mp.setattr(
        pool_registry,
        "AsyncIOMotorClient",
        AsyncMongoMockClient,
        raising=False,
    )

    try:
        yield
    finally:
        mp.undo()


@pytest.fixture(scope="session", autouse=True)
def _force_test_env_and_memory_secret_backend() -> None:
    """
    Apply a consistent test environment + in-memory secret backend for *all* tests.

    This prevents Windows CredWrite / win32ctypes errors by ensuring we never
    hit the OS credential manager during tests.
    """
    os.environ["EXECUTION_ENV"] = Environment.TEST.value
    os.environ["SECRET_BACKEND"] = "memory"
