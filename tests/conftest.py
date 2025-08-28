from __future__ import annotations

from typing import Generator, Callable, Iterable, Tuple, Dict
from fastapi.requests import Request

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
