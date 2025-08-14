from __future__ import annotations

from typing import Generator, Callable, Iterable

import pytest
import os
import importlib
import sys
from fastapi.testclient import TestClient
from sqlmodel import Session, delete

from src.main import app
from src.api.dependencies import get_execution_handler, get_job_handler
from src.persistance.db import engine
from src.persistance.table_definitions import (
    JobTable,
    MetaDataTable,
    ComponentTable,
    LayoutTable,
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
                "src.main",
                "src.components",  # component registry and implementations
                "src.api.routers",  # routers (schemas/jobs/execution/setup)
            )
        )
        importlib.invalidate_caches()
        import src.main as main

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


@pytest.fixture(scope="session", autouse=True)
def enable_stub_components():
    """
    Automatically run before any tests.
    Sets the registry mode to 'test' so stub components are visible.
    """
    os.environ["ETL_COMPONENT_MODE"] = "test"

    if "src.main" in sys.modules:
        import src.main

        importlib.reload(src.main)


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
    """
    Access the single JobHandler created in main.lifespan (app.state).
    Use this if a test needs to seed data without going through /jobs endpoints.
    """
    return client.app.state.job_handler


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
