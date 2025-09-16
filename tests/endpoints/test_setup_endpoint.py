from __future__ import annotations
from fastapi.testclient import TestClient


def test_setup_valid_key(client: TestClient) -> None:
    response = client.get("/setup", params={"key": "VALID_KEY"})
    assert response.status_code == 200
    assert response.json() is True


def test_setup_invalid_key(client: TestClient) -> None:
    response = client.get("/setup", params={"key": "INVALID"})
    assert response.status_code == 200
    assert response.json() is False
