from __future__ import annotations
from fastapi.testclient import TestClient

from etl_core.main import app


def test_setup_requires_authentication() -> None:
    with TestClient(app) as unauthenticated:
        response = unauthenticated.get("/setup")
        assert response.status_code == 401


def test_setup_returns_true_for_authorized_client(client: TestClient) -> None:
    response = client.get("/setup")
    assert response.status_code == 200
    assert response.json() is True
