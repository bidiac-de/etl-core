from __future__ import annotations

from typing import Generator

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, delete

from src.api.main import app
from src.api.dependencies import get_execution_handler, get_job_handler
from src.persistance.db import engine
from src.persistance.table_definitions import (
    JobTable,
    MetaDataTable,
    ComponentTable,
    LayoutTable,
)


@pytest.fixture()
def client() -> Generator[TestClient, None, None]:
    """
    Use TestClient as a context manager so FastAPI lifespan runs,
    which creates the shared handlers on app.state.
    """
    with TestClient(app) as c:
        yield c


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
